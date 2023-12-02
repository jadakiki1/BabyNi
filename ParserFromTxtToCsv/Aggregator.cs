using System;
using System.Data.Odbc;

namespace BabyNi
{
    public class Aggregator
    {
        private readonly string connectionString;

        public Aggregator(string connectionString)
        {
            this.connectionString = connectionString;
        }

        public void AggregateDataHourly()
        {
            try
            {
                using (OdbcConnection conn = new OdbcConnection(connectionString))
                {
                    conn.Open();
                    string aggregateQueryHourly = @"
                        INSERT INTO TRANS_MW_AGG_SLOT_HOURLY(
                            Datetime_key,
                            NeAlias,
                            NeType,
                            RFInputPower,
                            MaxRxLevel,
                            RSL_Deviation)
                        SELECT 
                            date_trunc('hour', rp.Datetime_key) as Datetime_key,
                            rf.NeAlias,
                            rf.NeType,
                            Max(rp.MaxRxLevel) as MaxRxLevel,
                            Max(rf.RFInputPower) as RFInputPower,
                            abs(Max(rp.MaxRxLevel)) - abs(Max(rf.RFInputPower)) as RSL_DEVIATION
                        FROM  TRANS_MW_ERC_PM_WAN_RFINPUTPOWER rf
                        INNER JOIN TRANS_MW_ERC_PM_TN_RADIO_LINK_POWER rp 
                        ON rf.NeAlias = rp.NeAlias AND rf.NeType = rp.NeType
                        GROUP BY date_trunc('hour', rp.Datetime_key), rf.NeAlias, rf.NeType;";

                    using (OdbcCommand cmd = new OdbcCommand(aggregateQueryHourly, conn))
                    {
                        int rowsAffected = cmd.ExecuteNonQuery();
                        Console.WriteLine($"Hourly aggregation complete. Rows affected: {rowsAffected}");
                    }
                }
            }
            catch (OdbcException odbcEx)
            {
                Console.WriteLine($"Database error during hourly aggregation: {odbcEx.Message}");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"An error occurred during hourly aggregation: {ex.Message}");
            }
        }

        public void AggregateDataDaily()
        {
            try
            {
                using (OdbcConnection conn = new OdbcConnection(connectionString))
                {
                    conn.Open();
                    string aggregateQueryDaily = @"
                        INSERT INTO TRANS_MW_AGG_SLOT_DAILY(
                            Datetime_key,
                            NeAlias,
                            NeType,
                            RFInputPower,
                            MaxRxLevel,
                            RSL_Deviation)
                        SELECT 
                            date_trunc('day', rp.Datetime_key) as Datetime_key,
                            rf.NeAlias,
                            rf.NeType,
                            Max(rp.MaxRxLevel) as MaxRxLevel,
                            Max(rf.RFInputPower) as RFInputPower,
                            abs(Max(rp.MaxRxLevel)) - abs(Max(rf.RFInputPower)) as RSL_DEVIATION
                        FROM  TRANS_MW_ERC_PM_WAN_RFINPUTPOWER rf
                        INNER JOIN TRANS_MW_ERC_PM_TN_RADIO_LINK_POWER rp 
                        ON rf.NeAlias = rp.NeAlias AND rf.NeType = rp.NeType
                        GROUP BY date_trunc('day', rp.Datetime_key), rf.NeAlias, rf.NeType;";

                    using (OdbcCommand cmd = new OdbcCommand(aggregateQueryDaily, conn))
                    {
                        int rowsAffected = cmd.ExecuteNonQuery();
                        Console.WriteLine($"Daily aggregation complete. Rows affected: {rowsAffected}");
                    }
                }
            }
            catch (OdbcException odbcEx)
            {
                Console.WriteLine($"Database error during daily aggregation: {odbcEx.Message}");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"An error occurred during daily aggregation: {ex.Message}");
            }
        }
    }
}
