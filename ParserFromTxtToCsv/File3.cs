using OfficeOpenXml;
using System.IO;
using System.Linq;
using System;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Http;
using System.Text.RegularExpressions;
using System.Globalization;
using System.Collections.Generic;
using Vertica.Data.Internal.DotNetDSI;
using Microsoft.Extensions.Configuration;
using System.Text;

namespace BabyNi
{
    public class File3
    {
        private readonly IConfiguration _configuration;
        private readonly RequestDelegate _next;
        private readonly FileSystemWatcher watcher;
        private readonly string outputFolderPath;
        private HashSet<string> processedFiles = new HashSet<string>();

        public File3(RequestDelegate next, IConfiguration configuration, FileSystemWatcher watcher, HashSet<string> processedFiles, string outputFolderPath)
        {
            _next = next;
            _configuration = configuration;
            this.watcher = watcher;
            this.outputFolderPath = outputFolderPath;

           
            this.watcher.Created += (sender, e) =>
            {
               Thread.Sleep(10);

                    if (e.Name.StartsWith("SOEM1_TN_RADIO_LINK_POWER"))
                    {

                        ConvertTextToCsv(e.FullPath);
                    }
                
            };
        }

        public async Task InvokeAsync(HttpContext context)
        {
            await _next(context);
        }


        private void ConvertTextToCsv(string txtFilePath)
        {
            Directory.CreateDirectory(outputFolderPath);

            var csvFilePath = Path.Combine(outputFolderPath, Path.GetFileNameWithoutExtension(txtFilePath) + ".csv");

                var lines = File.ReadAllLines(txtFilePath);
                var csvLines = new List<string>();
                string[] excludedColumns = { "nodename", "position", "IDLOGNUM" };

                var header = lines[0].Split(',');
                int timeColumnIndex = Array.IndexOf(header, "Time");
                int neAliasIndex = Array.IndexOf(header, "NeAlias");
                int neTypeIndex = Array.IndexOf(header, "NeType");
                int failureDescriptionIndex = Array.IndexOf(header, "FailureDescription");
                int objectColumnIndex = Array.IndexOf(header, "Object");

                if (timeColumnIndex == -1 || neAliasIndex == -1 || neTypeIndex == -1 || failureDescriptionIndex == -1 || objectColumnIndex == -1)
                {
                    throw new Exception("One of the required columns 'TIME', 'NEALIAS', 'NETYPE', 'FAILUREDESCRIPTION', or 'Object' was not found.");
                }



                var csvHeaderLine = "Network_SID,DateTime_Key," + String.Join(",", header.Where(c => !excludedColumns.Contains(c, StringComparer.OrdinalIgnoreCase))) + ",Link,TID,FarEndTID,Slot,Port";
                csvLines.Add(csvHeaderLine);


                foreach (var line in lines.Skip(1)) // Skip the header line
                {
                    var rowData = line.Split(',');

                    // Skip rows without a dash in the "FAILUREDESCRIPTION" column
                    if (!rowData[failureDescriptionIndex].Contains("-"))
                        continue;

                    string timeValue = rowData[timeColumnIndex];
                    string neAliasValue = rowData[neAliasIndex];
                    string neTypeValue = rowData[neTypeIndex];
                    int hash = (neAliasValue + neTypeValue).GetHashCode();
                    int networkSid = unchecked(hash == int.MinValue ? 0 : Math.Abs(hash));
                                 
                    
                    // Process and write the 'Link' value
                    string linkValue = ProcessObjectColumn(rowData[objectColumnIndex]);
                    // Process 'TID' and 'FarEndTID' values
                    string tidValue = ExtractTID(rowData[objectColumnIndex]);
                    string farEndTidValue = ExtractFarEndTID(rowData[objectColumnIndex]);
                    // Check if there are multiple slots to create duplicate rows
                    var slots = linkValue.Split('/')[0].Split('+');
                    string port = linkValue.Contains("/") ? linkValue.Split('/')[1] : ""; // Extracting the 'Port' value

                    foreach (var slot in slots)
                    {
                        var csvLine = new StringBuilder();
                        csvLine.Append(networkSid.ToString()).Append(",");
                        csvLine.Append(timeValue).Append(",");

                        // Write the existing data, including the 'Object' column
                        foreach (var columnValue in rowData.Where((value, index) => !excludedColumns.Contains(header[index], StringComparer.OrdinalIgnoreCase)))
                        {
                        csvLine.Append(columnValue).Append(",");
                        }

                        // Write the 'Link', 'TID', 'FarEndTID', and 'Slot' values
                        csvLine.Append(linkValue).Append(",");
                        csvLine.Append(tidValue).Append(",");
                        csvLine.Append(farEndTidValue).Append(",");
                        csvLine.Append(slot).Append(",");
                        csvLine.Append(port);

                        csvLines.Add(csvLine.ToString());

                    }

                }

            File.WriteAllLines(csvFilePath, csvLines);

            LoadDataIntoDatabase(csvFilePath);

            AggregateData();
 
        }


        private string ProcessObjectColumn(string objectValue)
        {
            // Remove trailing info starting with "_"
            var trimmedValue = objectValue.Split('_')[0];

            // Handle cases with "."
            if (trimmedValue.Contains("."))
            {
                var parts = trimmedValue.Split('/');
                if (parts.Length == 3)
                {
                    var slotAndPortParts = parts[1].Split(new char[] { '+' }, StringSplitOptions.RemoveEmptyEntries);
                    var processedParts = slotAndPortParts.Select(part =>
                    {
                        var subParts = part.Split('.');
                        return subParts.Length == 2 ? subParts[0] : part; // If '.' is present, take only the part before it
                    });
                    var portPart = slotAndPortParts.Select(part =>
                    {
                        var subParts = part.Split('.');
                        return subParts.Length == 2 ? subParts[1] : part; // If '.' is present, take only the part after it
                    });
                    // Concatenate the processed parts with "+" and append the last part (from parts[2])
                    trimmedValue = string.Join("+", processedParts) + "/" + parts[2];
                }
            }
            else
            {
                // If no ".", take value from middle till end as SLOT/PORT
                var parts = trimmedValue.Split(new char[] { '/' }, StringSplitOptions.RemoveEmptyEntries);
                if (parts.Length >= 2)
                {
                    trimmedValue = parts[1] + "/" + parts[2]; // Concatenate the second and third parts directly
                }
            }

            return trimmedValue;
        }

        private string ExtractTID(string objectValue)
        {
            // Split the string into parts using the underscore as a separator
            var parts = objectValue.Split('_');

            // We want to exclude the first and last parts, which are empty strings if the string starts and ends with an underscore
            // We also want to exclude the parts that correspond to the first and last segments split by slashes
            // The TID is expected to be the part right after the first non-empty segment
            var nonEmptyParts = parts.Where(part => !string.IsNullOrEmpty(part)).ToList();

            // The TID should be after the first segment and before the last segment
            if (nonEmptyParts.Count > 2)
            {
                // Return the second to last part which should be the TID
                return nonEmptyParts[^2]; // ^2 is an index from the end operator, getting the second to last item
            }

            return string.Empty; // If the structure is not as expected, return an empty string
        }


        private string ExtractFarEndTID(string objectValue)
        {
            // Split the string into parts using the underscore as a separator
            var parts = objectValue.Split('_');

            // The FarEndTID is expected to be the last part after the last non-empty segment
            var nonEmptyParts = parts.Where(part => !string.IsNullOrEmpty(part)).ToArray();

            // Check if there's at least one non-empty part which would be the FarEndTID
            if (nonEmptyParts.Length > 0)
            {
                // Return the last part which should be the FarEndTID
                return nonEmptyParts[^1]; // ^1 is an index from the end operator, getting the last item
            }

            return string.Empty; // If the structure is not as expected, return an empty string
        }

        private void LoadDataIntoDatabase(string csvFilePath)
        {
            string connectionString = _configuration.GetConnectionString("VerticaConnection");
            string tableName = "TRANS_MW_ERC_PM_TN_RADIO_LINK_POWER";

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

        private bool IsFileProcessed(string fileName)
        {
            return processedFiles.Contains(fileName);
        }

        private void OnFileDropped(object sender, FileSystemEventArgs e)
        {
            string specificFileName = "SOEM1_TN_RADIO_LINK_POWER_20200312_001500.txt";
            if (e.ChangeType == WatcherChangeTypes.Created && e.Name == specificFileName)
            {
                if (IsFileProcessed(e.Name))
                {
                    Console.WriteLine($"File already processed: {e.Name}");
                    // Optionally handle reprocessing here
                }
                else
                {
                    ConvertTextToCsv(e.FullPath);
                    // Mark the file as processed
                    processedFiles.Add(e.Name);
                }
            }
        }


    }
}
