using System;
using System.Data;
using System.Data.OleDb;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace RestApiWithOleDb.Controllers
{
    [ApiController]
    [Route("api/[controller]")]
    public class TransactionController : ControllerBase
    {
        private const string ConnectionString = "Provider=sqloledb;Data Source=ID-LPT-083;Initial Catalog=api_test;User Id=admin;Password=E3SCzYMpym2F;";
        private readonly ILogger<TransactionController> _logger;

        public TransactionController(ILogger<TransactionController> logger)
        {
            _logger = logger;
        }

        [HttpGet("history")]
        public async Task<IActionResult> GetHistory(string accountId, DateTime? startDate, DateTime? endDate)
        {
            try
            {
                using var connection = new OleDbConnection(ConnectionString);
                await connection.OpenAsync();

                // Start building the base query
                var query = @"SELECT * FROM BOS_History WHERE szAccountId = ?";

                // Add filters for date range if specified
                if (startDate.HasValue)
                {
                    query += " AND dtmTransaction >= ?";
                }
                if (endDate.HasValue)
                {
                    query += " AND dtmTransaction <= ?";
                }

                query += " ORDER BY dtmTransaction ASC;";

                using var cmd = new OleDbCommand(query, connection);

                // Add parameters in the exact order they appear in the query
                cmd.Parameters.AddWithValue("?", accountId); // AccountId parameter

                // Add the startDate parameter if provided
                if (startDate.HasValue)
                {
                    cmd.Parameters.AddWithValue("?", startDate.Value);
                }

                // Add the endDate parameter if provided
                if (endDate.HasValue)
                {
                    cmd.Parameters.AddWithValue("?", endDate.Value);
                }

                using var reader = await cmd.ExecuteReaderAsync();
                var historyList = new List<object>();

                while (await reader.ReadAsync())
                {
                    historyList.Add(new
                    {
                        TransactionId = reader["szTransactionId"].ToString(),
                        AccountId = reader["szAccountId"].ToString(),
                        CurrencyId = reader["szCurrencyId"].ToString(),
                        TransactionDate = Convert.ToDateTime(reader["dtmTransaction"]),
                        Amount = Convert.ToDecimal(reader["decAmount"]),
                        Note = reader["szNote"].ToString(),
                    });
                }

                return Ok(historyList);
            }
            catch (OleDbException ex)
            {
                // Log or inspect exception details here
                Console.WriteLine($"Error: {ex.Message}");
                Console.WriteLine($"Error Code: {ex.ErrorCode}");
                Console.WriteLine($"Stack Trace: {ex.StackTrace}");
                return StatusCode(StatusCodes.Status500InternalServerError, new { Message = ex.Message });
            }
        }



        [HttpPut("setor")]
        public async Task<IActionResult> Setor([FromBody] TransactionRequest request)
        {
            return await ProcessTransaction(request, TransactionType.Deposit);
        }

        [HttpPut("tarik")]
        public async Task<IActionResult> Tarik([FromBody] TransactionRequest request)
        {
            return await ProcessTransaction(request, TransactionType.Withdraw);
        }

        [HttpPut("transfer")]
        public async Task<IActionResult> Transfer([FromBody] TransferRequest request)
        {
            try
            {
                using var connection = new OleDbConnection(ConnectionString);
                await connection.OpenAsync();

                using var transaction = connection.BeginTransaction(IsolationLevel.ReadCommitted);
                try
                {
                    string transactionId = GenerateTransactionId(connection, transaction);

                    // Deduct from source account
                    if (!await UpdateBalance(connection, transaction, request.SourceAccountId, request.CurrencyId, -request.Amount))
                    {
                        return BadRequest(new { Message = "Insufficient balance." });
                    }

                    // Credit to target accounts
                    decimal distributedAmount = request.Amount / request.TargetAccountIds.Count;
                    foreach (var targetAccountId in request.TargetAccountIds)
                    {
                        await UpdateBalance(connection, transaction, targetAccountId, request.CurrencyId, distributedAmount);
                    }

                    // Insert history
                    await InsertHistory(connection, transaction, transactionId, request.SourceAccountId, request.CurrencyId, -request.Amount, "TRANSFER");
                    foreach (var targetAccountId in request.TargetAccountIds)
                    {
                        await InsertHistory(connection, transaction, transactionId, targetAccountId, request.CurrencyId, distributedAmount, "Transfer received");
                    }

                    transaction.Commit();
                    return Ok(new { TransactionId = transactionId });
                }
                catch
                {
                    transaction.Rollback();
                    throw;
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error during transfer transaction.");
                return StatusCode(StatusCodes.Status500InternalServerError, new { Message = ex.Message });
            }
        }

        private async Task<IActionResult> ProcessTransaction(TransactionRequest request, TransactionType type)
        {
            try
            {
                using var connection = new OleDbConnection(ConnectionString);
                await connection.OpenAsync();

                using var transaction = connection.BeginTransaction(IsolationLevel.ReadCommitted);
                try
                {
                    string transactionId = GenerateTransactionId(connection, transaction);
                    decimal adjustment = type == TransactionType.Deposit ? request.Amount : -request.Amount;

                    // Check balance for withdrawal
                    if (type == TransactionType.Withdraw && !await IsSufficientBalance(connection, transaction, request.AccountId, request.CurrencyId, request.Amount))
                    {
                        return BadRequest(new { Message = "Insufficient balance." });
                    }

                    // Update balance
                    await UpdateBalance(connection, transaction, request.AccountId, request.CurrencyId, adjustment);

                    // Insert history
                    await InsertHistory(connection, transaction, transactionId, request.AccountId, request.CurrencyId, adjustment, request.Note);

                    transaction.Commit();
                    return Ok(new { TransactionId = transactionId });
                }
                catch
                {
                    transaction.Rollback();
                    throw;
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error during transaction.");
                return StatusCode(StatusCodes.Status500InternalServerError, new { Message = ex.Message });
            }
        }

        private async Task<bool> UpdateBalance(OleDbConnection connection, OleDbTransaction transaction, string accountId, string currencyId, decimal amount)
        {
            string query = @"MERGE INTO BOS_Balance AS Target
                             USING (SELECT ? AS AccountId, ? AS CurrencyId, ? AS Amount) AS Source
                             ON Target.szAccountId = Source.AccountId AND Target.szCurrencyId = Source.CurrencyId
                             WHEN MATCHED THEN UPDATE SET Target.decAmount += Source.Amount
                             WHEN NOT MATCHED THEN INSERT (szAccountId, szCurrencyId, decAmount) VALUES (Source.AccountId, Source.CurrencyId, Source.Amount);";

            using var cmd = new OleDbCommand(query, connection, transaction);
            cmd.Parameters.Add(new OleDbParameter("AccountId", accountId));
            cmd.Parameters.Add(new OleDbParameter("CurrencyId", currencyId));
            cmd.Parameters.Add(new OleDbParameter("Amount", amount));
            await cmd.ExecuteNonQueryAsync();

            // Ensure balance is not negative
            return await IsSufficientBalance(connection, transaction, accountId, currencyId, 0);
        }

        private async Task<bool> IsSufficientBalance(OleDbConnection connection, OleDbTransaction transaction, string accountId, string currencyId, decimal requiredAmount)
        {
            string query = "SELECT decAmount FROM BOS_Balance WHERE szAccountId = ? AND szCurrencyId = ?;";
            using var cmd = new OleDbCommand(query, connection, transaction);
            cmd.Parameters.Add(new OleDbParameter("AccountId", accountId));
            cmd.Parameters.Add(new OleDbParameter("CurrencyId", currencyId));

            var balance = (decimal?)await cmd.ExecuteScalarAsync() ?? 0;
            return balance >= requiredAmount;
        }

        private async Task InsertHistory(OleDbConnection connection, OleDbTransaction transaction, string transactionId, string accountId, string currencyId, decimal amount, string note)
        {
            string query = "INSERT INTO BOS_History (szTransactionId, szAccountId, szCurrencyId, dtmTransaction, decAmount, szNote) VALUES (?, ?, ?, GETDATE(), ?, ?);";
            using var cmd = new OleDbCommand(query, connection, transaction);
            cmd.Parameters.Add(new OleDbParameter("TransactionId", transactionId));
            cmd.Parameters.Add(new OleDbParameter("AccountId", accountId));
            cmd.Parameters.Add(new OleDbParameter("CurrencyId", currencyId));
            cmd.Parameters.Add(new OleDbParameter("Amount", amount));
            cmd.Parameters.Add(new OleDbParameter("Note", note));
            await cmd.ExecuteNonQueryAsync();
        }

        private string GenerateTransactionId(OleDbConnection connection, OleDbTransaction transaction)
        {
            // Query to get the current counter value for '001-COU'
            string counterQuery = "SELECT iLastNumber FROM BOS_Counter WHERE szCounterId = '001-COU';";
            
            using (var cmd = new OleDbCommand(counterQuery, connection, transaction))
            {
                // Fetch the current counter value
                var counterValue = (long)cmd.ExecuteScalar();
                
                // Increment the counter value
                counterValue++;

                // Update the BOS_Counter table with the new counter value
                string updateCounterQuery = "UPDATE BOS_Counter SET iLastNumber = ? WHERE szCounterId = '001-COU';";
                
                using (var updateCmd = new OleDbCommand(updateCounterQuery, connection, transaction))
                {
                    updateCmd.Parameters.AddWithValue("?", counterValue);
                    updateCmd.ExecuteNonQuery();
                }

                // Generate the transaction ID
                string datePart = DateTime.Now.ToString("yyyyMMdd");
                string counterPart = counterValue.ToString("D5");  // 5-digit format
                string transactionId = $"{datePart}-00000.{counterPart}";  // Adding a ".00001" decimal part
                
                return transactionId;
            }
        }




        private enum TransactionType
        {
            Deposit,
            Withdraw
        }
    }

    public class TransactionRequest
    {
        public string AccountId { get; set; }
        public string CurrencyId { get; set; }
        public decimal Amount { get; set; }
        public string Note { get; set; }
    }

    public class TransferRequest
    {
        public string SourceAccountId { get; set; }
        public string CurrencyId { get; set; }
        public decimal Amount { get; set; }
        public List<string> TargetAccountIds { get; set; }
    }

    public class Program
    {
        public static void Main(string[] args)
        {
            CreateHostBuilder(args).Build().Run();
        }

        public static IHostBuilder CreateHostBuilder(string[] args) =>
            Host.CreateDefaultBuilder(args)
                .ConfigureWebHostDefaults(webBuilder =>
                {
                    webBuilder.UseStartup<Startup>(); // Use the Startup class for configuration
                });
    }
}
