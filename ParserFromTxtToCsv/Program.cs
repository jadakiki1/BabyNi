using BabyNi;
using System.IO;
using System.Collections.Generic; // Needed for HashSet
using Microsoft.AspNetCore.Builder; // Required for UseCors
using Microsoft.Extensions.DependencyInjection; // Required for AddCors

var builder = WebApplication.CreateBuilder(args);

var configuration = builder.Configuration;

// Add services to the container.
builder.Services.AddControllers();
builder.Services.AddEndpointsApiExplorer();
builder.Services.AddSwaggerGen();
builder.Services.AddSingleton<IConfiguration>(configuration);

// Configure CORS
builder.Services.AddCors(options =>
{
    options.AddDefaultPolicy(builder =>
    {
        builder.AllowAnyOrigin()
               .AllowAnyMethod()
               .AllowAnyHeader();
    });
});

var app = builder.Build();

if (app.Environment.IsDevelopment())
{
    app.UseSwagger();
    app.UseSwaggerUI();
}

// Use CORS policy
app.UseCors();

// Shared FileSystemWatcher
var watcher = new FileSystemWatcher(@"C:\Users\User\Desktop\Baby Ni\BackEnd\Watcher")
{
    EnableRaisingEvents = true,
    Filter = "*.txt"
};

// HashSet to keep track of processed files
HashSet<string> processedFiles = new HashSet<string>();

watcher.Created += (sender, e) =>
{
    if (processedFiles.Contains(e.Name))
    {
        Console.WriteLine($"File Already Processed: {e.Name} ... Overwriting");
    }
    else
    {
        Console.WriteLine($"File created: {e.FullPath}");
        processedFiles.Add(e.Name); // Add file name to processedFiles set
        // Logic to process the file
    }
};

watcher.Renamed += (sender, e) =>
{
    Console.WriteLine($"File renamed: {e.FullPath}");
    
};

watcher.Deleted += (sender, e) =>
{
    Console.WriteLine($"File deleted: {e.FullPath}");
    
};

// Pass the FileSystemWatcher and HashSet instance to the middleware
app.UseMiddleware<File3>(watcher, processedFiles, @"C:\Users\User\Desktop\Baby Ni\BackEnd\Parser");
app.UseMiddleware<File4>(watcher, processedFiles, @"C:\Users\User\Desktop\Baby Ni\BackEnd\Parser");

app.UseAuthorization();
app.MapControllers();
app.UseCors();
app.Run();
