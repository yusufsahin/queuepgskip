using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;

namespace FileCopyDemo
{
    // DB'den okuduğumuz işi temsil eden model
    public record FileCopyJobRow(long Id, string SourcePath, string DestinationPath);

    class Program
    {
        // Postgres connection string – kendine göre düzenle
        private const string ConnectionString =
            "Host=localhost;Port=5432;Database=filecopy_demo;Username=XXXXX;Password=XXXX";

        static async Task Main()
        {
            var cts = new CancellationTokenSource();

            Console.CancelKeyPress += (_, e) =>
            {
                e.Cancel = true;
                cts.Cancel();
            };

            Console.WriteLine("DB tabanlı FileCopy worker başlıyor. Ctrl+C ile durdurabilirsin.");

            while (!cts.IsCancellationRequested)
            {
                FileCopyJobRow? job = null;

                try
                {
                    job = await TryGetNextJobAsync(cts.Token);
                }
                catch (Exception ex)
                {
                    Console.WriteLine("⚠ TryGetNextJobAsync hata: " + ex.Message);
                    Console.WriteLine(ex.StackTrace);
                    await Task.Delay(TimeSpan.FromSeconds(2), cts.Token);
                    continue;
                }

                if (job is null)
                {
                    Console.WriteLine("İş yok, 2 sn bekleniyor...");
                    await Task.Delay(TimeSpan.FromSeconds(2), cts.Token);
                    continue;
                }

                Console.WriteLine($"Worker aldı: {job.SourcePath} -> {job.DestinationPath} (id={job.Id})");

                try
                {
                    await CopyFileAsync(job.SourcePath, job.DestinationPath);
                    await MarkJobDoneAsync(job.Id, cts.Token);
                    Console.WriteLine("  ✔ DONE");
                }
                catch (Exception ex)
                {
                    Console.WriteLine("  ✘ ERROR: " + ex.Message);
                    Console.WriteLine(ex.StackTrace);
                    await MarkJobFailedAsync(job.Id, ex, cts.Token);
                }
            }

            Console.WriteLine("Worker durduruldu.");
        }

        // Tek dosya kopyalama (Seviye 1 ile aynı mantık)
        static async Task CopyFileAsync(string sourcePath, string destinationPath)
        {
            var directory = Path.GetDirectoryName(destinationPath);
            if (!string.IsNullOrEmpty(directory))
            {
                Directory.CreateDirectory(directory);
            }

            await using var sourceStream = File.OpenRead(sourcePath);
            await using var destStream = File.Create(destinationPath);

            await sourceStream.CopyToAsync(destStream);
        }

        // DB'den bir tane pending job çek:
        //  - status='pending' olanlardan birini
        //  - FOR UPDATE SKIP LOCKED ile kilitle
        //  - status='processing' yapıp geri döndür
        static async Task<FileCopyJobRow?> TryGetNextJobAsync(CancellationToken ct)
        {
            const string sql = @"
WITH cte AS (
    SELECT id
    FROM file_copy_job
    WHERE status = 'pending'
    ORDER BY created_at
    FOR UPDATE SKIP LOCKED
    LIMIT 1
)
UPDATE file_copy_job j
SET status     = 'processing',
    updated_at = now()
FROM cte
WHERE j.id = cte.id
RETURNING j.id, j.source_path, j.destination_path;
";

            await using var conn = new NpgsqlConnection(ConnectionString);
            await conn.OpenAsync(ct);

            await using var tx = await conn.BeginTransactionAsync(ct);
            await using var cmd = new NpgsqlCommand(sql, conn, tx);
            await using var reader = await cmd.ExecuteReaderAsync(ct);

            if (!await reader.ReadAsync(ct))
            {
                await reader.CloseAsync();
                await tx.CommitAsync(ct);
                return null; // pending iş yok
            }

            var job = new FileCopyJobRow(
                reader.GetInt64(0),
                reader.GetString(1),
                reader.GetString(2));

            await reader.CloseAsync();
            await tx.CommitAsync(ct);

            return job;
        }

        // İş başarılı → status='done'
        static async Task MarkJobDoneAsync(long id, CancellationToken ct)
        {
            const string sql = @"
UPDATE file_copy_job
SET status = 'done',
    updated_at = now()
WHERE id = @id;
";

            await using var conn = new NpgsqlConnection(ConnectionString);
            await conn.OpenAsync(ct);

            await using var cmd = new NpgsqlCommand(sql, conn);
            cmd.Parameters.AddWithValue("id", id);

            await cmd.ExecuteNonQueryAsync(ct);
        }

        // İş hatalı → status='failed', last_error doldur
        static async Task MarkJobFailedAsync(long id, Exception ex, CancellationToken ct)
        {
            const string sql = @"
UPDATE file_copy_job
SET status     = 'failed',
    last_error = @err,
    updated_at = now()
WHERE id = @id;
";

            await using var conn = new NpgsqlConnection(ConnectionString);
            await conn.OpenAsync(ct);

            await using var cmd = new NpgsqlCommand(sql, conn);
            cmd.Parameters.AddWithValue("id", id);
            cmd.Parameters.AddWithValue("err", ex.Message);

            await cmd.ExecuteNonQueryAsync(ct);
        }
    }
}
