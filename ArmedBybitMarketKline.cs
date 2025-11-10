using Bybit.Api.Market;

public record ArmedBybitMarketKline : BybitMarketKline
{
    public decimal RSI { get; set; } = 0;
    public (decimal Hist, decimal Macd, decimal Signal) MACD { get; set; } = (0, 0, 0);
    public decimal SMA { get; set; } = 0;
    public CandleType Type { get; set; }
    public decimal ChangePercent { get; set; }
    public decimal DownTail { get; set; }      //in percents
    public decimal UpperTail{ get; set; }        //in percents
    public ArmedBybitMarketKline(BybitMarketKline kline)
    {
        OpenTime = kline.OpenTime;
        OpenPrice = kline.OpenPrice;
        ClosePrice = kline.ClosePrice;
        HighPrice = kline.HighPrice;
        LowPrice = kline.LowPrice;
        Volume = kline.Volume;
        QuoteVolume = kline.QuoteVolume;
        ChangePercent = (ClosePrice - OpenPrice) / (OpenPrice / 100);
        Type = ChangePercent < 0 ? CandleType.Bear : ChangePercent > 0 ? CandleType.Bull : CandleType.Zero;
        if (Type == CandleType.Bull)
        {
            UpperTail = Math.Abs(ClosePrice - HighPrice) / (ClosePrice / 100);
            DownTail = (OpenPrice - LowPrice) / (OpenPrice / 100);
        }
        else if (Type == CandleType.Bear)
        {
            UpperTail = Math.Abs(OpenPrice - HighPrice) / (OpenPrice / 100);
            DownTail = (ClosePrice - LowPrice) / (ClosePrice / 100);
        }
        else
        {
            UpperTail = DownTail = 0;
        }
    }
    public ArmedBybitMarketKline()
    { }
    public override string ToString()
    {
        return string.Format("{8}:\n Open: {0}, Close: {1}, Max: {2}, Low: {3}, Volume: {4}, Change percent: {5} %,\n RSI: {6}, MACD: {7}, SMA: {9}",
        OpenPrice, ClosePrice, HighPrice, LowPrice, Math.Round(Volume / 1000000, 2) + " M", Math.Round(ChangePercent, 2), RSI,
         $"{MACD.Hist}, {MACD.Macd}, {MACD.Signal}", OpenTime, Math.Round(SMA, 3));

    }
}
