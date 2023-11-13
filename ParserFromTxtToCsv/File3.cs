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

namespace ParserFromTxtToCsv
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
                if (e.Name.StartsWith("SOEM1_TN_RADIO_LINK_POWER"))
                {
                    Console.WriteLine($"Converted Successfully");
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

            using (var writer = new StreamWriter(csvFilePath))
            {
                var lines = File.ReadAllLines(txtFilePath);
                string[] excludedColumns = { "nodename", "position", "IDLOGNUM" };

                var header = lines[0].Split(',');
                int neAliasIndex = Array.IndexOf(header, "NeAlias");
                int neTypeIndex = Array.IndexOf(header, "NeType");
                int failureDescriptionIndex = Array.IndexOf(header, "FailureDescription");
                int objectColumnIndex = Array.IndexOf(header, "Object");

                if (neAliasIndex == -1 || neTypeIndex == -1 || failureDescriptionIndex == -1 || objectColumnIndex == -1)
                {
                    throw new Exception("One of the required columns 'NEALIAS', 'NETYPE', 'FAILUREDESCRIPTION', or 'Object' was not found.");
                }

                // Extract datetime part from the filename
                var dateTimePart = ExtractDateTimeFromFilename(Path.GetFileName(txtFilePath));

                // Write the new "NETWORK_SID" and "DATETIME STAMP" column headers
                writer.Write("Network_SID,DateTime_Key,");

                foreach (var column in header.Where(c => !excludedColumns.Contains(c, StringComparer.OrdinalIgnoreCase)))
                {
                    writer.Write(column + ",");
                }

                writer.WriteLine("Link,TID,FarEndTID,Slot,Port");
                

                foreach (var line in lines.Skip(1)) // Skip the header line
                {
                    var rowData = line.Split(',');

                    // Skip rows without a dash in the "FAILUREDESCRIPTION" column
                    if (!rowData[failureDescriptionIndex].Contains("-"))
                        continue;

                    // Compute the "NETWORK_SID"
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
                        writer.Write(networkSid.ToString() + ",");
                        writer.Write(dateTimePart + ",");

                        // Write the existing data, including the 'Object' column
                        foreach (var columnValue in rowData.Where((value, index) => !excludedColumns.Contains(header[index], StringComparer.OrdinalIgnoreCase)))
                        {
                            writer.Write(columnValue + ",");
                        }

                        // Write the 'Link', 'TID', 'FarEndTID', and 'Slot' values
                        writer.Write(linkValue + ",");
                        writer.Write(tidValue + ",");
                        writer.Write(farEndTidValue + ",");
                        writer.Write(slot + ",");
                        writer.Write(port);

                        writer.WriteLine();
                       

                        try
                        {
                            string connectionString = _configuration.GetConnectionString("VerticaConnection");
                            string tableName = "SOEM1_TN_RADIO_LINK_POWER";

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
        
    }
}
