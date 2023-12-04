using OfficeOpenXml;
using System.IO;
using System.Linq;
using System;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Http;
using System.Text.RegularExpressions;
using System.Globalization;
using System.Collections.Generic;
using Microsoft.Extensions.Configuration;
using System.Text;

namespace BabyNi
{
    public class File4
    {
        private readonly IConfiguration _configuration;
        private readonly RequestDelegate _next;
        private readonly FileSystemWatcher watcher;
        private readonly string outputFolderPath;
        private HashSet<string> processedFiles = new HashSet<string>();

        public File4(RequestDelegate next, IConfiguration configuration, FileSystemWatcher watcher, HashSet<string> processedFiles, string outputFolderPath)
        {
            _next = next;
            _configuration = configuration;
            this.watcher = watcher;
            this.outputFolderPath = outputFolderPath;

            this.watcher.Created += (sender, e) =>
            {

                Thread.Sleep(10);
                if (e.Name.StartsWith("SOEM1_TN_RFInputPower"))
                {
                    
                    ConvertAnotherTextToCsv(e.FullPath);
                }
            };

        }

        public async Task InvokeAsync(HttpContext context)
        {
            await _next(context);
        }

        private void ConvertAnotherTextToCsv(string txtFilePath)
        {
            Directory.CreateDirectory(outputFolderPath);

            var csvFilePath = Path.Combine(outputFolderPath, Path.GetFileNameWithoutExtension(txtFilePath) + ".csv");

            var lines = File.ReadAllLines(txtFilePath);
            var csvLines = new List<string>();
            string[] excludedColumns = { "Position", "MeanRxLevel1m", "IdLogNum", "FailureDescription" };

            var header = lines[0].Split(',');
            int timeColumnIndex = Array.IndexOf(header, "Time");
            int neAliasIndex = Array.IndexOf(header, "NeAlias");
            int neTypeIndex = Array.IndexOf(header, "NeType");
            int farEndTidIndex = Array.IndexOf(header, "FarEndTID");
            int objectIndex = Array.IndexOf(header, "Object");

            if (timeColumnIndex == -1 || neAliasIndex == -1 || neTypeIndex == -1 || objectIndex == -1)
            {
                throw new Exception("TIME,NeAlias, NeType or Object column not found.");
            }

           
            var csvHeaderLine = "Network_SID,DateTime_Key," + String.Join(",", header.Where(c => !excludedColumns.Contains(c, StringComparer.OrdinalIgnoreCase))) + ",Slot,Port";
            csvLines.Add(csvHeaderLine);

            foreach (var line in lines.Skip(1)) // Skip the header line
            {
                var rowData = line.Split(',');

                if (farEndTidIndex != -1 && rowData[farEndTidIndex].Trim() == "----")
                {
                    continue;
                }

                string timeValue = rowData[timeColumnIndex];
                string neAliasValue = rowData[neAliasIndex];
                string neTypeValue = rowData[neTypeIndex];
                int networkSidHash = (neAliasValue + neTypeValue).GetHashCode();
                string networkSid = unchecked(networkSidHash == int.MinValue ? "0" : Math.Abs(networkSidHash).ToString());

                var csvLine = new StringBuilder();
                csvLine.Append(networkSid).Append(",");
                csvLine.Append(timeValue).Append(",");

                foreach (var columnValue in rowData.Where((value, index) => !excludedColumns.Contains(header[index])))
                {
                    csvLine.Append(columnValue).Append(",");
                }

                // Process Object values for Slot and Port
                var objectValue = rowData[objectIndex];
                var slotValue = "";
                var portValue = "";

                if (objectValue.Contains("."))
                {
                    var preDotPart = objectValue.Split('.')[0];

                    var lastSlashIndex = preDotPart.LastIndexOf('/');
                    if (lastSlashIndex != -1 && lastSlashIndex < preDotPart.Length - 1)
                    {
                        slotValue = preDotPart.Substring(lastSlashIndex + 1);
                    }

                    var parts = objectValue.Split(new[] { '.' }, StringSplitOptions.RemoveEmptyEntries);
                    if (parts.Length > 1)
                    {
                        var portParts = parts[1].Split(new[] { '/' }, StringSplitOptions.RemoveEmptyEntries);
                        if (portParts.Length > 0)
                        {
                            portValue = portParts[0];
                        }
                    }
                }
                else
                {
                    if (objectValue.Contains("/"))
                    {
                        var endIndex = objectValue.Contains(".") ? objectValue.IndexOf(".") : objectValue.Length;
                        slotValue = objectValue.Substring(0, endIndex).Split('+').First() + "+";
                    }
                }

                csvLine.Append(slotValue).Append(",");
                csvLine.Append(portValue);

                csvLines.Add(csvLine.ToString());

                
            }

            File.WriteAllLines(csvFilePath, csvLines);

            LoadDataIntoDatabase(csvFilePath);

            AggregateData();

            
        }

        private void LoadDataIntoDatabase(string csvFilePath)
        {
            string connectionString = _configuration.GetConnectionString("VerticaConnection");
            string tableName = "TRANS_MW_ERC_PM_WAN_RFINPUTPOWER";

            DataLoader loader = new DataLoader(connectionString);
            loader.LoadData(csvFilePath, tableName);


        }

        private void AggregateData()
        {
            string connectionString = _configuration.GetConnectionString("VerticaConnection");

            Aggregator aggregator = new Aggregator(connectionString);
            aggregator.AggregateDataHourly();
            aggregator.AggregateDataDaily();

        }

    }
}
