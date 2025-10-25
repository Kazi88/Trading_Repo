using System.Text.Json;

public class Config
{
    public string? BYBIT_API_KEY { get; set; }
    public string? BYBIT_API_SECRET { get; set; }
    public string? DB_CONNECTION_STRING { get; set; }
    public static Config? GetConfig() => JsonSerializer.Deserialize<Config>(File.ReadAllText("/home/ilya/Документы/Trading_Repo/.gitignore/config.json"));
}