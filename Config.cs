using System.Text.Json;

public class Config
{
    public string? BYBIT_API_KEY { get; set; }
    public string? BYBIT_API_SECRET { get; set; }
    public string? DB_CONNECTION_STRING { get; set; }
    public static Config? GetConfig() => JsonSerializer.Deserialize<Config>(File.ReadAllText("/home/ilya/Documents/Trading_Repository/Trading_Repo/config.json"));
}