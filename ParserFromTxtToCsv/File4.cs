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

namespace ParserFromTxtToCsv
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
                if (e.Name.StartsWith("SOEM1_TN_RFInputPower"))
                {
                    Console.WriteLine($"Converted Successfully");
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

            using (var writer = new StreamWriter(csvFilePath))
            {
                var lines = File.ReadAllLines(txtFilePath);
                string[] excludedColumns = { "position", "MeanRxLevel1m", "IdLogNum", "FailureDescription" };

                var header = lines[0].Split(',');
                int neAliasIndex = Array.IndexOf(header, "NeAlias");
                int neTypeIndex = Array.IndexOf(header, "NeType");
                int farEndTidIndex = Array.IndexOf(header, "FarEndTID");
                int objectIndex = Array.IndexOf(header, "Object");

                if (neAliasIndex == -1 || neTypeIndex == -1 || objectIndex == -1)
                {
                    throw new Exception("NeAlias, NeType or Object column not found.");
                }

                // Extract datetime part from the filename
                var dateTimePart = ExtractDateTimeFromFilename(Path.GetFileName(txtFilePath));

                // Write the 'Network_SID' and 'DateTime_Key' headers first, then the rest of the headers
                writer.Write("Network_SID,DateTime_Key,");
                foreach (var column in header.Where(c => !excludedColumns.Contains(c)))
                {
                    writer.Write(column + ",");
                }
                writer.WriteLine("Slot,Port"); // Finish the header line

                // Write the rows
                foreach (var line in lines.Skip(1)) // Skip the header line
                {
                    var rowData = line.Split(',');

                    // Skip rows with "----" in the FarEndTID column
                    if (farEndTidIndex != -1 && rowData[farEndTidIndex].Trim() == "----")
                    {
                        continue; // Skip this record
                    }

                    // Calculate Network_SID based on NeAlias and NeType
                    string neAliasValue = rowData[neAliasIndex];
                    string neTypeValue = rowData[neTypeIndex];
                    int networkSidHash = (neAliasValue + neTypeValue).GetHashCode();
                    string networkSid = unchecked(networkSidHash == int.MinValue ? "0" : Math.Abs(networkSidHash).ToString());


                    // Write the Network_SID and DateTime_Key first
                    writer.Write(networkSid.ToString() + ",");
                    writer.Write(dateTimePart + ",");

                    // Write the rest of the data, excluding the specific columns
                    foreach (var columnValue in rowData.Where((value, index) => !excludedColumns.Contains(header[index])))
                    {
                        writer.Write(columnValue + ",");
                    }
                    var objectValue = rowData[objectIndex];
                    var slotValue = "";
                    var portValue = "";
                    if (objectValue.Contains("/"))
                    {
                        var endIndex = objectValue.Contains(".") ? objectValue.IndexOf(".") : objectValue.Length;
                        slotValue = objectValue.Substring(0, endIndex) + "+";

                        // Extract the Port value
                        var parts = objectValue.Split(new[] { '.' }, StringSplitOptions.RemoveEmptyEntries);
                        if (parts.Length > 1)
                        {
                            var portParts = parts[1].Split(new[] { '/' }, StringSplitOptions.RemoveEmptyEntries);
                            if (portParts.Length > 0)
                            {
                                portValue = portParts[0]; // Get the number after the "."
                            }
                        }
                    }

                    writer.Write(slotValue + ","); // Write the Slot value
                    writer.WriteLine(portValue);


                    try
                    {
                        string connectionString = _configuration.GetConnectionString("VerticaConnection");
                        string tableName = "SOEM1_TN_RFInputPower";

                        DataLoader loader = new DataLoader(connectionString);
                        loader.LoadData(csvFilePath, tableName);
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine($"Failed to load data to Vertica: {ex.Message}");
                    }

                    processedFiles.Add(Path.GetFileName(txtFilePath));
                }
            }
        }
    
        private string ExtractDateTimeFromFilename(string filename)
        {
            // Pattern matches "YYYYMMDD_HHMMSS" in the filename
            var match = Regex.Match(filename, @"\d{8}_\d{6}");
            if (!match.Success)
            {
                throw new Exception("Filename does not contain a valid datetime part.");
            }

            // Extract the date and time parts
            var datePart = match.Value.Substring(0, 8);
            var timePart = match.Value.Substring(9, 6);

            // Parse and convert the date and time parts into the required format "dd-MM-yyyy HH:mm:ss"
            DateTime dateValue = DateTime.ParseExact(datePart, "yyyyMMdd", CultureInfo.InvariantCulture);
            DateTime timeValue = DateTime.ParseExact(timePart, "HHmmss", CultureInfo.InvariantCulture);

            return $"{dateValue:dd-MM-yyyy} {timeValue:HH:mm:ss}";
        }

    }
}
