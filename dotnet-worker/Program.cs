using StackExchange.Redis;
using Npgsql;
using Microsoft.Extensions.Configuration;

var config = new ConfigurationBuilder()
  .AddJsonFile("appsettings.json", optional: false)
  .AddEnvironmentVariables()
  .Build();

string redisUrl = config["Redis:Url"] ?? "redis:6379";
string stream = config["Redis:Stream"] ?? "votes";
string group = config["Redis:Group"] ?? "workers";
string consumer = config["Redis:Consumer"] ?? $"worker-{Guid.NewGuid()}";
string connStr = config["Db:Conn"]!;

var mux = await ConnectionMultiplexer.ConnectAsync(redisUrl);
var db = mux.GetDatabase();

// create group if not exists
try { await db.StreamCreateConsumerGroupAsync(stream, group, "$"); } catch { }

Console.WriteLine("Worker up");
while (true)
{
  var entries = await db.StreamReadGroupAsync(stream, group, consumer, count: 64, noAck: false, block: 5000);
  if (entries.Length == 0) continue;

  await using var npg = new NpgsqlConnection(connStr);
  await npg.OpenAsync();

  foreach (var e in entries)
  {
    try
    {
      var kv = e.Values.ToDictionary(x => x.Name!, x => x.Value!.ToString());
      long electionId = long.Parse(kv["election_id"]);
      long candidateId = long.Parse(kv["candidate_id"]);
      string voterId = kv["voter_id"];
      string voterCardHash = kv["voter_card_hash"];

      await using var tx = await npg.BeginTransactionAsync();

      // upsert voter
      await using (var cmd = new NpgsqlCommand(
        "insert into voters(id, voter_card_hash) values($1,$2) " +
        "on conflict (id) do update set voter_card_hash = excluded.voter_card_hash", npg))
      {
        cmd.Parameters.AddWithValue(voterId);
        cmd.Parameters.AddWithValue(voterCardHash);
        await cmd.ExecuteNonQueryAsync();
      }

      // insert vote unique per election voter
      int rows;
      await using (var cmd = new NpgsqlCommand(
        "insert into votes(election_id, candidate_id, voter_id) values($1,$2,$3) on conflict do nothing", npg))
      {
        cmd.Parameters.AddWithValue(electionId);
        cmd.Parameters.AddWithValue(candidateId);
        cmd.Parameters.AddWithValue(voterId);
        rows = await cmd.ExecuteNonQueryAsync();
      }

      await tx.CommitAsync();

      if (rows == 0)
      {
        Console.WriteLine($"duplicate vote ignored election={electionId} voter={voterId}");
      }

      await db.StreamAcknowledgeAsync(stream, group, e.Id);
    }
    catch (Exception ex)
    {
      Console.Error.WriteLine($"process error {e.Id}: {ex.Message}");
      // leave unacked for retry
    }
  }
}
