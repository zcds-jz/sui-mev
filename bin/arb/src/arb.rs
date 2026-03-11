//! Example:
//! cargo run -r --bin arb run --coin-type \
//!     "0xa8816d3a6e3136e86bc2873b1f94a15cadc8af2703c075f2d546c2ae367f4df9::ocean::OCEAN"

use std::{
    collections::{HashMap, HashSet},
    fmt,
    str::FromStr,
    sync::Arc,
    time::{Duration, Instant},
};

use async_trait::async_trait;
use clap::Parser;
use dex_indexer::types::Protocol;
use eyre::{ensure, ContextCompat, Result};
use itertools::Itertools;
use object_pool::ObjectPool;
use simulator::{HttpSimulator, SimulateCtx, Simulator};
use sui_sdk::{SuiClientBuilder, SUI_COIN_TYPE};
use sui_types::{
    base_types::{ObjectID, ObjectRef, SuiAddress},
    transaction::TransactionData,
};
use tokio::task::JoinSet;
use tracing::{debug, info, instrument, Instrument};
use utils::coin;

use crate::{
    common::get_latest_epoch,
    common::graph::WeightedDigraph,
    common::search::{golden_section_search_maximize, SearchGoal},
    defi::{Defi, Dex, Path, TradeType},
    types::Source,
    HttpConfig,
};

#[derive(Clone, Debug, Parser)]
pub struct Args {
    #[arg(long)]
    pub coin_type: String,

    #[arg(long)]
    pub pool_id: Option<String>,

    #[arg(long, default_value = "")]
    pub sender: String,

    #[command(flatten)]
    pub http_config: HttpConfig,
}

pub async fn run(args: Args) -> Result<()> {
    mev_logger::init_console_logger_with_directives(None, &["arb=debug", "dex_indexer=debug"]);

    info!("Running arb with {:?}", args);
    let rpc_url = args.http_config.rpc_url.clone();
    let ipc_path = args.http_config.ipc_path.clone();

    let sender = SuiAddress::from_str(&args.sender).map_err(|e| eyre::eyre!(e))?;

    let simulator_pool = ObjectPool::new(1, move || {
        tokio::runtime::Runtime::new()
            .unwrap()
            .block_on(async { Box::new(HttpSimulator::new(&rpc_url, &ipc_path).await) as Box<dyn Simulator> })
    });

    let arb = Arb::new(
        &args.http_config.rpc_url,
        Arc::new(simulator_pool),
        SearchConfig::default(),
    )
    .await?;
    let sui = SuiClientBuilder::default().build(&args.http_config.rpc_url).await?;
    let gas_coins = coin::get_gas_coin_refs(&sui, sender, None).await?;
    let epoch = get_latest_epoch(&sui).await?;
    let sim_ctx = SimulateCtx::new(epoch, vec![]);
    let pool_id = args.pool_id.as_deref().map(ObjectID::from_hex_literal).transpose()?;

    let result = arb
        .find_opportunity(
            sender,
            &args.coin_type,
            pool_id,
            gas_coins,
            sim_ctx,
            true,
            Source::Public,
        )
        .await?;

    info!("{result:#?}");
    Ok(())
}

#[derive(Debug)]
pub struct ArbResult {
    pub create_trial_ctx_duration: Duration,
    pub grid_search_duration: Duration,
    pub gss_duration: Option<Duration>,
    pub best_trial_result: TrialResult,
    pub cache_misses: u64,
    pub source: Source,
    pub tx_data: TransactionData,
}

#[derive(Clone, Debug)]
pub struct SearchConfig {
    pub graph_search_enabled: bool,
    pub graph_hop_tolerance: usize,
    pub graph_max_paths_per_side: usize,
}

impl Default for SearchConfig {
    fn default() -> Self {
        Self {
            graph_search_enabled: false,
            graph_hop_tolerance: 0,
            graph_max_paths_per_side: 32,
        }
    }
}

pub struct Arb {
    defi: Defi,
    search_config: SearchConfig,
}

impl Arb {
    pub async fn new(
        http_url: &str,
        simulator_pool: Arc<ObjectPool<Box<dyn Simulator>>>,
        search_config: SearchConfig,
    ) -> Result<Self> {
        let defi = Defi::new(http_url, simulator_pool).await?;
        Ok(Self { defi, search_config })
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn find_opportunity(
        &self,
        sender: SuiAddress,
        coin_type: &str,
        pool_id: Option<ObjectID>,
        gas_coins: Vec<ObjectRef>,
        sim_ctx: SimulateCtx,
        use_gss: bool,
        source: Source,
    ) -> Result<ArbResult> {
        let gas_price = sim_ctx.epoch.gas_price;

        let (ctx, create_trial_ctx_duration) = {
            let timer = Instant::now();
            let ctx = Arc::new(
                TrialCtx::new(
                    self.defi.clone(),
                    sender,
                    coin_type,
                    pool_id,
                    gas_coins.clone(),
                    sim_ctx,
                    self.search_config.clone(),
                )
                .await?,
            );

            (ctx, timer.elapsed())
        };
        info!(
            coin_type,
            buy_path_candidates = ctx.buy_paths.len(),
            sell_path_candidates = ctx.sell_paths.len(),
            graph_search_enabled = self.search_config.graph_search_enabled,
            "机会搜索上下文初始化完成"
        );

        // Grid search
        let starting_grid = 1_000_000u64; // 0.001 SUI
        let mut cache_misses = 0;
        let (mut max_trial_res, grid_search_duration) = {
            let timer = Instant::now();
            let mut joinset = JoinSet::new();
            for inc in 1..11 {
                let ctx = ctx.clone();
                let grid = starting_grid.checked_mul(10u64.pow(inc)).context("Grid overflow")?;

                joinset.spawn(async move { ctx.trial(grid).await }.in_current_span());
            }

            let mut max_trial_res = TrialResult::default();
            while let Some(Ok(trial_res)) = joinset.join_next().await {
                // debug!(?trial_res, "Grid searching");
                if let Ok(trial_res) = trial_res {
                    if trial_res.cache_misses > cache_misses {
                        cache_misses = trial_res.cache_misses;
                    }
                    if trial_res > max_trial_res {
                        max_trial_res = trial_res;
                    }
                }
            }
            (max_trial_res, timer.elapsed())
        };

        ensure!(
            max_trial_res.profit > 0,
            "cache_misses: {}. No profitable grid found",
            cache_misses
        );

        let gss_duration = if use_gss {
            // GSS
            let timer = Instant::now();
            let upper_bound = max_trial_res.amount_in.saturating_mul(10);
            let lower_bound = max_trial_res.amount_in.saturating_div(10);

            let goal = TrialGoal;
            let (_, _, trial_res) = golden_section_search_maximize(lower_bound, upper_bound, goal, &ctx).await;
            if trial_res.cache_misses > cache_misses {
                cache_misses = trial_res.cache_misses;
            }
            if trial_res > max_trial_res {
                max_trial_res = trial_res;
            }

            Some(timer.elapsed())
        } else {
            None
        };

        ensure!(
            max_trial_res.profit > 0,
            "cache_misses: {}. No profitable trade path found",
            cache_misses
        );

        let TrialResult {
            amount_in,
            trade_path,
            profit,
            ..
        } = &max_trial_res;

        let mut source = source;
        if source.deadline().is_some() {
            source = source.with_arb_found_time(utils::current_time_ms());
        }
        // TODO make bid_amount configurable
        source = source.with_bid_amount(*profit / 10 * 9);

        let tx_data = self
            .defi
            .build_final_tx_data(sender, *amount_in, trade_path, gas_coins, gas_price, source)
            .await?;

        Ok(ArbResult {
            create_trial_ctx_duration,
            grid_search_duration,
            gss_duration,
            best_trial_result: max_trial_res,
            cache_misses,
            source,
            tx_data,
        })
    }
}

pub struct TrialCtx {
    defi: Defi,
    sender: SuiAddress,
    coin_type: String,
    pool_id: Option<ObjectID>,
    buy_paths: Vec<Path>,
    sell_paths: Vec<Path>,
    gas_coins: Vec<ObjectRef>,
    sim_ctx: SimulateCtx,
    search_config: SearchConfig,
}

impl TrialCtx {
    pub async fn new(
        defi: Defi,
        sender: SuiAddress,
        coin_type: &str,
        pool_id: Option<ObjectID>,
        gas_coins: Vec<ObjectRef>,
        sim_ctx: SimulateCtx,
        search_config: SearchConfig,
    ) -> Result<Self> {
        let raw_buy_paths = defi.find_buy_paths(coin_type).await?;
        ensure!(!raw_buy_paths.is_empty(), "no buy paths found for {}", coin_type);

        let raw_sell_paths = defi.find_sell_paths(coin_type).await?;
        ensure!(!raw_sell_paths.is_empty(), "no sell paths found for {}", coin_type);

        let (buy_paths, sell_paths, graph_stats) =
            optimize_paths_with_graph(raw_buy_paths, raw_sell_paths, coin_type, pool_id, &search_config);
        info!(
            coin_type,
            graph_search_enabled = search_config.graph_search_enabled,
            buy_before = graph_stats.buy_before,
            buy_after = graph_stats.buy_after,
            sell_before = graph_stats.sell_before,
            sell_after = graph_stats.sell_after,
            shortest_buy_weight = ?graph_stats.shortest_buy_weight,
            shortest_sell_weight = ?graph_stats.shortest_sell_weight,
            graph_buy_found = graph_stats.graph_buy_found,
            graph_sell_found = graph_stats.graph_sell_found,
            "候选路径准备完成"
        );

        if pool_id.is_some() {
            let buy_paths_contain_pool = buy_paths.iter().any(|p| p.contains_pool(pool_id));
            let sell_paths_contain_pool = sell_paths.iter().any(|p| p.contains_pool(pool_id));
            ensure!(
                buy_paths_contain_pool || sell_paths_contain_pool,
                "no paths found for the fluctuating pool: {:?}",
                pool_id
            );
        }

        Ok(Self {
            defi,
            sender,
            coin_type: coin_type.to_string(),
            pool_id,
            buy_paths,
            sell_paths,
            gas_coins,
            sim_ctx,
            search_config,
        })
    }

    #[instrument(
        name = "trial",
        skip_all,
        fields(
            in = %format!("{:<15}", (amount_in as f64 / 1_000_000_000.0)),
            len = %format!("{:<2}", self.buy_paths.len()),
            action="init"
        )
    )]
    pub async fn trial(&self, amount_in: u64) -> Result<TrialResult> {
        tracing::Span::current().record("action", "buy");

        let timer = Instant::now();
        let best_buy_res = self
            .defi
            .find_best_path_exact_in(
                &self.buy_paths,
                self.sender,
                amount_in,
                TradeType::Swap,
                &self.gas_coins,
                &self.sim_ctx,
            )
            .await?;
        let buy_elapsed = timer.elapsed();

        let timer = Instant::now();
        // append sell paths to the best buy path
        let best_buy_path = best_buy_res.path;
        let buy_path_contains_pool = best_buy_path.contains_pool(self.pool_id);
        let trade_paths = self
            .sell_paths
            .iter()
            .filter_map(|p| {
                // - buy_path and sell_path should not have common pools
                // - either buy_path or sell_path should contain the swapped_pool
                if best_buy_path.is_disjoint(p) && (buy_path_contains_pool || p.contains_pool(self.pool_id)) {
                    let mut path = best_buy_path.clone();
                    path.path.extend(p.path.clone());
                    Some(path)
                } else {
                    None
                }
            })
            .collect_vec();
        if trade_paths.is_empty() {
            info!(
                coin_type = %self.coin_type,
                amount_in,
                buy_path = %format_path(&best_buy_path),
                pool_id = ?self.pool_id,
                "闭环套利路径组合完成，但无可用候选"
            );
        } else {
            info!(
                coin_type = %self.coin_type,
                amount_in,
                pool_id = ?self.pool_id,
                buy_path = %format_path(&best_buy_path),
                closed_loop_count = trade_paths.len(),
                sample_closed_loop = %format_path(&trade_paths[0]),
                "闭环套利路径组合完成"
            );
        }
        ensure!(
            !trade_paths.is_empty(),
            "no trade paths found for coin {}, pool_id: {:?}",
            self.coin_type,
            self.pool_id
        );

        tracing::Span::current().record("action", "sell");
        let best_trade_res = self
            .defi
            .find_best_path_exact_in(
                &trade_paths,
                self.sender,
                amount_in,
                TradeType::Flashloan,
                &self.gas_coins,
                &self.sim_ctx,
            )
            .await?;

        let sell_elapsed = timer.elapsed();
        debug!(
            coin_type = ?self.coin_type,
            result = %best_trade_res,
            ?buy_elapsed,
            ?sell_elapsed,
            graph_search_enabled = self.search_config.graph_search_enabled,
            "trial result"
        );

        let profit = best_trade_res.profit();
        if profit <= 0 {
            return Ok(TrialResult::default());
        }

        let result = TrialResult::new(
            &self.coin_type,
            amount_in,
            profit as u64,
            best_trade_res.path,
            best_trade_res.cache_misses,
        );

        Ok(result)
    }
}

fn format_path(path: &Path) -> String {
    path.path
        .iter()
        .map(|dex| {
            let coin_in = dex.coin_in_type();
            let coin_out = dex.coin_out_type();
            let coin_in = coin_in.split("::").last().unwrap_or(coin_in.as_str());
            let coin_out = coin_out.split("::").last().unwrap_or(coin_out.as_str());
            format!("{:?}:{}->{}@{}", dex.protocol(), coin_in, coin_out, dex.object_id())
        })
        .join(" | ")
}

#[derive(Debug, Default)]
struct GraphPathStats {
    buy_before: usize,
    sell_before: usize,
    buy_after: usize,
    sell_after: usize,
    shortest_buy_weight: Option<i64>,
    shortest_sell_weight: Option<i64>,
    graph_buy_found: bool,
    graph_sell_found: bool,
}

fn optimize_paths_with_graph(
    buy_paths: Vec<Path>,
    sell_paths: Vec<Path>,
    coin_type: &str,
    pool_id: Option<ObjectID>,
    search_config: &SearchConfig,
) -> (Vec<Path>, Vec<Path>, GraphPathStats) {
    let raw_buy_paths = buy_paths;
    let raw_sell_paths = sell_paths;

    let mut stats = GraphPathStats {
        buy_before: raw_buy_paths.len(),
        sell_before: raw_sell_paths.len(),
        buy_after: raw_buy_paths.len(),
        sell_after: raw_sell_paths.len(),
        ..GraphPathStats::default()
    };

    if !search_config.graph_search_enabled {
        return (raw_buy_paths, raw_sell_paths, stats);
    }

    let mut best_edge_by_coin_pair: HashMap<(String, String), Box<dyn Dex>> = HashMap::new();
    for path in raw_buy_paths.iter().chain(raw_sell_paths.iter()) {
        for dex in &path.path {
            let key = (dex.coin_in_type(), dex.coin_out_type());
            let should_replace = best_edge_by_coin_pair
                .get(&key)
                .map(|existing| dex.liquidity() > existing.liquidity())
                .unwrap_or(true);
            if should_replace {
                best_edge_by_coin_pair.insert(key, dex.clone());
            }
        }
    }

    if best_edge_by_coin_pair.is_empty() {
        return (raw_buy_paths, raw_sell_paths, stats);
    }

    let mut graph = WeightedDigraph::default();
    for ((coin_in, coin_out), dex) in &best_edge_by_coin_pair {
        graph.add_edge(coin_in.clone(), coin_out.clone(), edge_weight(dex.as_ref()));
    }

    let graph_buy_path = graph.bellman_ford(SUI_COIN_TYPE).and_then(|sp| {
        stats.shortest_buy_weight = sp.distance_to(coin_type);
        sp.path_to(coin_type)
            .and_then(|nodes| nodes_to_path(&nodes, &best_edge_by_coin_pair))
    });
    let graph_sell_path = graph.bellman_ford(coin_type).and_then(|sp| {
        stats.shortest_sell_weight = sp.distance_to(SUI_COIN_TYPE);
        sp.path_to(SUI_COIN_TYPE)
            .and_then(|nodes| nodes_to_path(&nodes, &best_edge_by_coin_pair))
    });

    stats.graph_buy_found = graph_buy_path.is_some();
    stats.graph_sell_found = graph_sell_path.is_some();

    let truncate = pool_id.is_none();
    let mut buy_paths = prioritize_paths(
        raw_buy_paths.clone(),
        graph_buy_path,
        stats.shortest_buy_weight,
        search_config.graph_hop_tolerance,
        search_config.graph_max_paths_per_side,
        truncate,
    );
    let mut sell_paths = prioritize_paths(
        raw_sell_paths.clone(),
        graph_sell_path,
        stats.shortest_sell_weight,
        search_config.graph_hop_tolerance,
        search_config.graph_max_paths_per_side,
        truncate,
    );

    if buy_paths.is_empty() {
        buy_paths = paths_fallback_by_weight(&raw_buy_paths);
    }
    if sell_paths.is_empty() {
        sell_paths = paths_fallback_by_weight(&raw_sell_paths);
    }

    if pool_id.is_some()
        && !buy_paths.iter().any(|p| p.contains_pool(pool_id))
        && !sell_paths.iter().any(|p| p.contains_pool(pool_id))
    {
        if let Some(path) = raw_buy_paths
            .iter()
            .find(|p| p.contains_pool(pool_id))
            .cloned()
            .or_else(|| raw_sell_paths.iter().find(|p| p.contains_pool(pool_id)).cloned())
        {
            let mut seen: HashSet<Vec<ObjectID>> = buy_paths
                .iter()
                .map(path_signature)
                .chain(sell_paths.iter().map(path_signature))
                .collect();
            let sig = path_signature(&path);
            if seen.insert(sig) {
                if path.coin_out_type() == SUI_COIN_TYPE {
                    sell_paths.push(path);
                } else {
                    buy_paths.push(path);
                }
            }
        }
    }

    stats.buy_after = buy_paths.len();
    stats.sell_after = sell_paths.len();
    (buy_paths, sell_paths, stats)
}

fn prioritize_paths(
    original_paths: Vec<Path>,
    graph_path: Option<Path>,
    shortest_weight: Option<i64>,
    hop_tolerance: usize,
    max_paths_per_side: usize,
    truncate: bool,
) -> Vec<Path> {
    if original_paths.is_empty() {
        return vec![];
    }

    let mut ranked = original_paths.clone();
    ranked.sort_by_key(path_weight);
    ranked.retain(|p| !p.is_empty());

    if let Some(shortest_weight) = shortest_weight {
        let max_allowed = shortest_weight.saturating_add(hop_tolerance as i64);
        ranked.retain(|p| path_weight(p) <= max_allowed);
        if ranked.is_empty() {
            ranked = paths_fallback_by_weight(&original_paths);
        }
    }

    let mut deduped = vec![];
    let mut seen = HashSet::new();
    if let Some(path) = graph_path {
        insert_unique_path(&mut deduped, &mut seen, path);
    }
    for path in ranked {
        insert_unique_path(&mut deduped, &mut seen, path);
    }

    if truncate {
        deduped.truncate(max_paths_per_side.max(1));
    }

    deduped
}

fn paths_fallback_by_weight(paths: &[Path]) -> Vec<Path> {
    let mut ranked = paths.to_vec();
    ranked.sort_by_key(path_weight);
    ranked.retain(|p| !p.is_empty());
    if ranked.is_empty() {
        return paths.to_vec();
    }
    ranked
}

fn insert_unique_path(deduped: &mut Vec<Path>, seen: &mut HashSet<Vec<ObjectID>>, path: Path) {
    let sig = path_signature(&path);
    if seen.insert(sig) {
        deduped.push(path);
    }
}

fn nodes_to_path(nodes: &[String], edge_map: &HashMap<(String, String), Box<dyn Dex>>) -> Option<Path> {
    if nodes.len() < 2 {
        return None;
    }

    let mut path = Vec::with_capacity(nodes.len().saturating_sub(1));
    for pair in nodes.windows(2) {
        let key = (pair[0].clone(), pair[1].clone());
        let dex = edge_map.get(&key)?.clone();
        path.push(dex);
    }
    Some(Path::new(path))
}

fn path_signature(path: &Path) -> Vec<ObjectID> {
    path.path.iter().map(|dex| dex.object_id()).collect()
}

fn path_weight(path: &Path) -> i64 {
    path.path.iter().map(|dex| edge_weight(dex.as_ref())).sum::<i64>()
}

fn edge_weight(dex: &dyn Dex) -> i64 {
    // Prioritize fewer hops first, then fee/slippage/gas.
    let hop_penalty = 100i64;
    let fee_penalty_bps = protocol_fee_bps(dex.protocol()) as i64;
    let liquidity_penalty_bps = low_liquidity_penalty_bps(dex.liquidity()) as i64;
    let gas_penalty_bps = 3i64;

    hop_penalty + fee_penalty_bps + liquidity_penalty_bps + gas_penalty_bps
}

fn protocol_fee_bps(protocol: Protocol) -> u64 {
    match protocol {
        Protocol::DeepbookV2 | Protocol::DeepbookV3 => 8,
        Protocol::Aftermath => 10,
        Protocol::Cetus | Protocol::Turbos | Protocol::KriyaClmm | Protocol::FlowxClmm => 30,
        Protocol::KriyaAmm | Protocol::FlowxAmm | Protocol::SuiSwap | Protocol::BlueMove => 30,
        Protocol::Navi => 5,
        _ => 30,
    }
}

fn low_liquidity_penalty_bps(liquidity: u128) -> u64 {
    // Lightweight slippage proxy: lower liquidity => larger penalty.
    let liq = liquidity.max(1);
    ((1_000_000_000_000u128 / liq).min(120)) as u64
}

#[derive(Debug, Default, Clone)]
pub struct TrialResult {
    pub coin_type: String,
    pub amount_in: u64,
    pub profit: u64,
    pub trade_path: Path,
    pub cache_misses: u64,
}

impl PartialOrd for TrialResult {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        self.profit.partial_cmp(&other.profit)
    }
}

impl PartialEq for TrialResult {
    fn eq(&self, other: &Self) -> bool {
        self.profit == other.profit
    }
}

impl TrialResult {
    pub fn new(coin_type: &str, amount_in: u64, profit: u64, trade_path: Path, cache_misses: u64) -> Self {
        Self {
            coin_type: coin_type.to_string(),
            amount_in,
            profit,
            trade_path,
            cache_misses,
        }
    }
}

impl fmt::Display for TrialResult {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "TrialResult {{ coin_type: {}, amount_in: {}, profit: {}, trade_path: {:?} ... }}",
            self.coin_type, self.amount_in, self.profit, self.trade_path
        )
    }
}

pub struct TrialGoal;

#[async_trait]
impl SearchGoal<TrialCtx, u64, TrialResult> for TrialGoal {
    async fn evaluate(&self, amount_in: u64, ctx: &TrialCtx) -> (u64, TrialResult) {
        let trial_res = ctx.trial(amount_in).await.unwrap_or_default();
        (trial_res.profit, trial_res)
    }
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use simulator::{DBSimulator, HttpSimulator, Simulator};
    use sui_types::base_types::SuiAddress;

    use super::*;
    use crate::config::tests::{TEST_ATTACKER, TEST_HTTP_URL};

    #[tokio::test]
    async fn test_find_best_trade_path() {
        mev_logger::init_console_logger_with_directives(None, &["arb=debug"]);

        let simulator_pool = ObjectPool::new(1, move || {
            tokio::runtime::Runtime::new()
                .unwrap()
                .block_on(async { Box::new(HttpSimulator::new(&TEST_HTTP_URL, &None).await) as Box<dyn Simulator> })
        });

        let start = Instant::now();

        let sender = SuiAddress::from_str(TEST_ATTACKER).unwrap();
        let sui = SuiClientBuilder::default().build(TEST_HTTP_URL).await.unwrap();
        let epoch = get_latest_epoch(&sui).await.unwrap();
        let sim_ctx = SimulateCtx::new(epoch, vec![]);

        let gas_coins = coin::get_gas_coin_refs(&sui, sender, None).await.unwrap();
        let arb = Arb::new(TEST_HTTP_URL, Arc::new(simulator_pool), SearchConfig::default())
            .await
            .unwrap();
        let coin_type = "0xce7ff77a83ea0cb6fd39bd8748e2ec89a3f41e8efdc3f4eb123e0ca37b184db2::buck::BUCK";

        let arb_res = arb
            .find_opportunity(
                sender,
                coin_type,
                None,
                gas_coins,
                sim_ctx.clone(),
                true,
                Source::Public,
            )
            .await
            .unwrap();
        info!(?arb_res, "Best trade path");

        info!("Creating DB simulator ...");
        let db_sim: Arc<dyn Simulator> = Arc::new(DBSimulator::new_default_slow().await);
        info!("DB simulator created in {:?}", start.elapsed());

        let tx_data = arb_res.tx_data;
        let http_sim: Arc<dyn Simulator> = Arc::new(HttpSimulator::new(TEST_HTTP_URL, &None).await);

        let http_res = http_sim.simulate(tx_data.clone(), sim_ctx.clone()).await.unwrap();
        info!(?http_res, "🧀 HTTP simulation result");

        let db_res = db_sim.simulate(tx_data, sim_ctx).await.unwrap();
        info!(?db_res, "🧀 DB simulation result");
    }
}
