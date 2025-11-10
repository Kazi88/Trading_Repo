using Bybit.Api.Market;
using TALib;

public static class IndicatorsObserver
{
    public static List<ArmedBybitMarketKline> GetArmedKlines(this List<BybitMarketKline> klines)
    {
        List<ArmedBybitMarketKline> output = new(klines.Count);
        foreach (var kline in klines) output.Add(new(kline));
        return output.GetRSI().GetMACD().GetSMA();
    }
    
    public static List<ArmedBybitMarketKline> GetArmedKlines(this List<ArmedBybitMarketKline> klines)
    {
        return klines.GetRSI().GetMACD().GetSMA();
    }
    public static List<ArmedBybitMarketKline> GetRSI(this List<ArmedBybitMarketKline> klines, int period = 14)
    {
        ReadOnlySpan<double> closePrices = klines.Select(candle => (double)candle.ClosePrice).ToArray().AsSpan();
        Span<double> result = new double[closePrices.Length];
        Range range;
        Functions.Rsi(closePrices, new Range(0, closePrices.Length - 1), result, out range, period);
        for (int i = range.Start.Value; i < range.End.Value; i++)
        {
            klines[i].RSI = (decimal)result[i - range.Start.Value];
        }
        return klines;
    }

    public static List<ArmedBybitMarketKline> GetMACD(this List<ArmedBybitMarketKline> klines, int fastPeriod = 12, int slowPeriod = 26, int signalPeriod = 9)
    {
        ReadOnlySpan<float> closePrices = klines.Select(candle => (float)candle.OpenPrice).ToArray().AsSpan();
        Span<float> hist = new float[klines.Count];
        Span<float> macd = new float[klines.Count];
        Span<float> signal = new float[klines.Count];
        Range range;
        Functions.Macd(closePrices, new Range(0, closePrices.Length - 1), macd, signal, hist, out range, fastPeriod, slowPeriod, signalPeriod);
        for (int i = range.Start.Value; i < range.End.Value; i++)
        {
            int current = i - range.Start.Value;
            klines[i - 1].MACD = ((decimal)hist[current], (decimal)macd[current], (decimal)signal[current]);
        }
        return klines;
    }

    public static List<ArmedBybitMarketKline> GetSMA(this List<ArmedBybitMarketKline> klines, int period = 100)
    {
        ReadOnlySpan<float> closePrices = klines.Select(candle => (float)candle.ClosePrice).ToArray().AsSpan();
        Span<float> sma = new float[closePrices.Length];
        Range outRange;
        Functions.Sma(closePrices, new Range(0, closePrices.Length - 1), sma, out outRange, period);
        for (int i = outRange.Start.Value; i < outRange.End.Value; i++)
        {
            klines[i].SMA = (decimal)sma[i - outRange.Start.Value];
        }
        return klines;
    }
    
    public static async Task SetIndicators(DbRelation relation, string tokenName)
    {
        //
    }

}

public enum CandleType
{
    Bull,
    Bear,
    Zero
}
