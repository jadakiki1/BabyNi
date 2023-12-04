
﻿using System;
using System.Data.Odbc;
using System.Collections.Generic;

using System.Data.Odbc;
using Vertica.Data.Internal.DotNetDSI;


namespace BabyNi
{
    public class AggregatedDataDto
    {
        public DateTime DateTime_Key { get; set; }
        public string NeAlias { get; set; }
        public string NeType { get; set; }
        public double RFInputPower { get; set; }
        public double MaxRxLevel { get; set; }
        public double RSL_Deviation { get; set; }
    }

    public class DataAggregationService
    {
        public List<AggregatedDataDto> GetAggregatedData(string tableName, string aggColumn)
        {
            var aggregatedDataList = new List<AggregatedDataDto>();

            using (OdbcConnection conn = new OdbcConnection("Driver={Vertica};Server=10.10.4.231;Database=test;User=bootcamp7;Password=bootcamp72023;"))
            {
                conn.Open();

                string query = $"SELECT Datetime_key, {aggColumn}, MAX(RFInputPower) as RFInputPower, MAX(MaxRxLevel) as MaxRxLevel, MAX(RSL_Deviation) as RSL_Deviation FROM {tableName} GROUP BY Datetime_key, {aggColumn}";

                using (OdbcCommand cmd = new OdbcCommand(query, conn))
                using (OdbcDataReader reader = cmd.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        var data = new AggregatedDataDto
                        {
                            DateTime_Key = reader.GetDateTime(reader.GetOrdinal("DateTime_Key")),

                            RFInputPower = reader.GetDouble(reader.GetOrdinal("RFInputPower")),
                            MaxRxLevel = reader.GetDouble(reader.GetOrdinal("MaxRxLevel")),
                            RSL_Deviation = reader.GetDouble(reader.GetOrdinal("RSL_Deviation"))
                        };

                        if (aggColumn.Equals("NeAlias", StringComparison.OrdinalIgnoreCase))
                        {
                            data.NeAlias = reader.GetString(reader.GetOrdinal("NeAlias"));
                        }
                        else if (aggColumn.Equals("NeType", StringComparison.OrdinalIgnoreCase))
                        {
                            data.NeType = reader.GetString(reader.GetOrdinal("NeType"));
                        }

                        aggregatedDataList.Add(data);
                    }
                }
            }

            return aggregatedDataList;
        }
    }
}
