using System.Data.Odbc;
using Vertica.Data.Internal.DotNetDSI;

namespace BabyNi
{
    public class AggregatedDataDto
    {
        public DateTime Time { get; set; }
        public string NeAlias { get; set; }
        public string NeType { get; set; }
        public double RFInputPower { get; set; }
        public double MaxRxLevel { get; set; }
        public double RSL_Deviation { get; set; }
    }

    public class DataAggregationService
    {

        public List<AggregatedDataDto> GetAggregatedData(string tableName)
        {
            var aggregatedDataList = new List<AggregatedDataDto>();

            using (OdbcConnection conn = new OdbcConnection("Driver={Vertica};Server=10.10.4.231;Database=test;User=bootcamp7;Password=bootcamp72023;"))
            {
                conn.Open();
                string query = $"SELECT Time, NeAlias, NeType, RFInputPower, MaxRxLevel, RSL_Deviation FROM {tableName}";
                using (OdbcCommand cmd = new OdbcCommand(query, conn))
                using (OdbcDataReader reader = cmd.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        var data = new AggregatedDataDto
                        {
                            Time = reader.GetDateTime(reader.GetOrdinal("Time")),
                            NeAlias = reader.GetString(reader.GetOrdinal("NeAlias")),
                            NeType = reader.GetString(reader.GetOrdinal("NeType")),
                            RFInputPower = reader.GetDouble(reader.GetOrdinal("RFInputPower")),
                            MaxRxLevel = reader.GetDouble(reader.GetOrdinal("MaxRxLevel")),
                            RSL_Deviation = reader.GetDouble(reader.GetOrdinal("RSL_Deviation"))
                        };
                        aggregatedDataList.Add(data);
                    }
                }
            }

            return aggregatedDataList;
        }
    }
    

}
