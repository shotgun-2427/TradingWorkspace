from trading_engine.core import orchestrate_portfolio_backtests, orchestrate_portfolio_simulations
import polars as pl
from research.utils import _get_metric

def orchestrate_marginal_simulations(config, model_insights, model_backtests, main_portfolio_backtests, prices):
    results = {}
    
    if not config.models or not config.optimizers:
        return results
    
    def calculate_marginal_values(main_df, reduced_df=None):
        if main_df is None:
            return {}
            
        if reduced_df is None:
            return {
                'excess_return': _get_metric(main_df, 'annualized_return'),
                'sharpe_improvement': _get_metric(main_df, 'sharpe_ratio'),
                'sortino_improvement': _get_metric(main_df, 'sortino_ratio'),
                'drawdown_improvement': -_get_metric(main_df, 'max_drawdown'),
                'volatility_change': _get_metric(main_df, 'annualized_volatility'),
                'turnover_increase': _get_metric(main_df, 'portfolio_turnover'),
                'additional_costs': _get_metric(main_df, 'cumulative_slippage_cost'),
            }
        return {
            'excess_return': _get_metric(main_df, 'annualized_return') - _get_metric(reduced_df, 'annualized_return'),
            'sharpe_improvement': _get_metric(main_df, 'sharpe_ratio') - _get_metric(reduced_df, 'sharpe_ratio'),
            'sortino_improvement': _get_metric(main_df, 'sortino_ratio') - _get_metric(reduced_df, 'sortino_ratio'),
            'drawdown_improvement': _get_metric(reduced_df, 'max_drawdown') - _get_metric(main_df, 'max_drawdown'),
            'volatility_change': _get_metric(main_df, 'annualized_volatility') - _get_metric(reduced_df, 'annualized_volatility'),
            'turnover_increase': _get_metric(main_df, 'portfolio_turnover') - _get_metric(reduced_df, 'portfolio_turnover'),
            'additional_costs': _get_metric(main_df, 'cumulative_slippage_cost') - _get_metric(reduced_df, 'cumulative_slippage_cost'),
        }

    def create_marginal_df(optimizers, main_backtests, reduced_backtests=None):
        optimizer_results = {}
        for name in optimizers:
            if name in main_backtests and (reduced_backtests is None or name in reduced_backtests):
                main_df = main_backtests[name]['backtest_metrics'] if main_backtests else None
                reduced_df = reduced_backtests[name]['backtest_metrics'] if reduced_backtests else None
                
                values = calculate_marginal_values(main_df, reduced_df)
                values['net_benefit'] = values['excess_return'] - values['additional_costs']
                
                rows = [{'metric': k, 'value': v} for k, v in values.items()]
                if not rows:
                    continue
                optimizer_results[name] = pl.DataFrame(rows)
        
        return optimizer_results if optimizer_results else {}

    if len(config.models) <= 1:
        return results
    
    for name in config.models:
        reduced_insights = {k: v for k, v in model_insights.items() if k != name}
        reduced_backtests = {k: v for k, v in model_backtests.items() if k != name}
        
        if reduced_backtests:
            portfolio_insights = orchestrate_portfolio_backtests(
                model_insights=reduced_insights,
                backtest_results=reduced_backtests,
                universe=config.universe,
                optimizers=config.optimizers,
            )
            
            portfolio_backtests = orchestrate_portfolio_simulations(
                prices=prices,
                portfolio_insights=portfolio_insights,
                initial_value=1_000_000.0,
            )
            
            model_results = create_marginal_df(config.optimizers, main_portfolio_backtests, portfolio_backtests)
        else:
            model_results = create_marginal_df(config.optimizers, main_portfolio_backtests)
        
        if model_results:
            results[name] = model_results
    
    return results