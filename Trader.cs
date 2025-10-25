using ApiSharp.Authentication;
using Bybit.Api;
using Bybit.Api.Enums;
public class FuturesTrader
{
    public readonly BybitRestApiClient _apiClient;
    private decimal walletBalance { get; set; } = 0;
    private decimal Quantity;
    public FuturesTrader(string keyApi, string apiSecret, bool isTestNet)
    {
        BybitRestApiClientOptions apiOptions = new(new ApiCredentials(keyApi, apiSecret))
        {
            BaseAddress = isTestNet ? BybitAddress.TestNet.RestApiAddress : BybitAddress.MainNet.RestApiAddress,
        };
        _apiClient = new(apiOptions);
    }
    public async Task<decimal> GetCurrentPrice(string futureName)
    {
        try
        {
            var tickerResult = await _apiClient.Market.GetLinearTickersAsync(futureName);
            return tickerResult.Data.Last().LastPrice;
        }
        catch (Exception ex)
        {
            Console.WriteLine(ex.Message);
            return -1;
        }
    }

    public async Task SetLeverage(string futureName, int buyLeverage, int sellLeverage)
    {
        await _apiClient.Position.SetLeverageAsync(BybitCategory.Linear, futureName, buyLeverage, sellLeverage);
    }
    public async Task ClosePosition(string futureName, int leverage, BybitOrderSide orderSide, decimal quantity)
    {
        int counter = 0;
        await SetLeverage(futureName, leverage, leverage);
        var b = await _apiClient.Position.GetPositionsAsync(BybitCategory.Linear, futureName);
        var position = b.Data?.First()?.Side;
        while (position == BybitPositionSide.Sell)
        {
            var order = await _apiClient.Trade.PlaceOrderAsync
            (
                BybitCategory.Linear,
                futureName,
                orderSide == BybitOrderSide.Buy ? BybitOrderSide.Sell : BybitOrderSide.Buy,
                BybitOrderType.Market,
                quantity
            );
            b = await _apiClient.Position.GetPositionsAsync(BybitCategory.Linear, "BTCUSDT");
            Console.WriteLine(b.Data.First().PositionBalance);
            Console.WriteLine(b.Data.First().Side);
            Console.WriteLine(++counter);
            b = await _apiClient.Position.GetPositionsAsync(BybitCategory.Linear, "BTCUSDT");
            await Task.Delay(100);
        }
        Console.WriteLine("$$$$$$$$$$$$$$CLOSED$$$$$$$$$$$$$$$");
    }

    public async Task PlaceOrder(string futureName, int leverage, BybitOrderSide orderSide)
    {
        try
        {
            await SetLeverage(futureName, leverage, leverage);
            await UpdateCurrentBalance();
            if (walletBalance != 0)
            {
                Console.WriteLine("Wallet balance: " + walletBalance);
                decimal marketPrice = await GetCurrentPrice(futureName);
                Console.WriteLine("Market price: " + marketPrice);
                decimal quantity = await CalcuLateQuantity(walletBalance, leverage, marketPrice, futureName);
                var order = await _apiClient.Trade.PlaceOrderAsync
            (
                BybitCategory.Linear,
                futureName,
                orderSide,
                BybitOrderType.Market,
                quantity
            );
                Console.WriteLine(order.Data.OrderId);
            }
            else Console.WriteLine("Balance is 0");
        }
        catch (Exception ex)
        {
            Console.WriteLine(ex.Message);
        }
        async Task<decimal> CalcuLateQuantity(decimal walletBalance, int leverage, decimal marketPrice, string futureName)
        {
            try
            {
                var futureInfo = await _apiClient.Market.GetLinearInstrumentsAsync(futureName);
                if (futureInfo is not null)
                {
                    var lotSize = futureInfo!.Data!.First().LotSizeFilter;
                    (decimal minQty, decimal maxQty, decimal qtyStep) =
                    (lotSize.MinimumOrderQuantity, lotSize.MaximumOrderQuantity, lotSize.QuantityStep);
                    Quantity = Math.Floor(walletBalance * leverage / marketPrice / qtyStep) * qtyStep;
                    Console.WriteLine("returned " + Quantity);
                    return Quantity;
                }
            }
            catch (Exception ex) { Console.WriteLine(ex.Message); return 0; }
            return 0;
        }
    }
    public async Task UpdateCurrentBalance()                    // UNFINISHED
    {
        try
        {
            var balances = await _apiClient.Account.GetBalancesAsync();
            // walletBalance = balances.Data.Last().TotalEquity - 30 ?? 0;
        }
        catch (Exception ex)
        {
            Console.WriteLine(ex.Message);
        }
    }
}