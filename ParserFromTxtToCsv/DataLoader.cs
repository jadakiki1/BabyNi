using System;
using System.Data.Odbc;

namespace BabyNi
{
    public class DataLoader
    {
        private readonly string connectionString;

        public DataLoader(string connectionString)
        {
            this.connectionString = connectionString;
        }

        public void LoadData(string csvFilePath, string tableName)
        {
            try
            {
                using (OdbcConnection conn = new OdbcConnection(connectionString))
                {
                    conn.Open();
                    string copyCommand = $"COPY {tableName} FROM LOCAL '{csvFilePath}' DELIMITER ',' DIRECT";

                    using (OdbcCommand cmd = new OdbcCommand(copyCommand, conn))
                    {
                        int rowsAffected = cmd.ExecuteNonQuery();
                        Console.WriteLine($"Data loaded successfully. Rows affected: {rowsAffected}");
                    }
                }
            }
            catch (OdbcException odbcEx)
            {
                // Handle database related errors
                Console.WriteLine($"Database error occurred: {odbcEx.Message}");
            }
            catch (Exception ex)
            {
                // Handle all other errors
                Console.WriteLine($"An error occurred: {ex.Message}");
            }
        }
    }
}
