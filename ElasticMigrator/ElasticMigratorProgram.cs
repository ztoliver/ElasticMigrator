using System;
using System.Collections.Generic;
using System.Configuration;
using System.Linq;
using Nest;

namespace ElasticMigrator
{
    internal class ElasticMigratorProgram
    {
        private const int MaxDocumentsProcessed = 100000;
        private static readonly string ElasticEndpoint = ConfigurationManager.AppSettings["ElasticEndpoint"];
        private static readonly ElasticClient Client = new ElasticClient(new Uri(ElasticEndpoint));

        private static void Main()
        {
            BeginMigration();
            MigrateIndex();
            Console.Read();
        }

        private static void MigrateIndex()
        {
            Console.WriteLine("Enter index name to migrate:");
            var indexFrom = Console.ReadLine();
            FindFromIndex(indexFrom);
            Console.WriteLine($"Enter index name you want to migrate data from '{indexFrom}' to:");
            var indexTo = Console.ReadLine();
            FindOrCreateToIndex(indexTo);
            var indexFromDocuments = Client.Search<object>(s => s
                .Index(indexFrom)
                .AllTypes()
                .Query(q => q.MatchAll())
                .Size(MaxDocumentsProcessed));
            Index(indexFromDocuments.Documents, indexTo);                 
        }

        private static void Index(IEnumerable<object> indexFromDocuments, string indexTo)
        {
            var retreivedDocuments = indexFromDocuments as IList<object> ?? indexFromDocuments.ToList();
            var indexAction = Client.IndexManyAsync(retreivedDocuments, indexTo);
            if (indexAction.Result.IsValid && indexAction.IsCompleted)
            {
                Console.WriteLine($"Successfully indexed {retreivedDocuments.Count} documents to index '{indexTo}'.");
            }
        }

        private static void FindOrCreateToIndex(string indexTo)
        {
            var indexFound = Client.GetIndex(indexTo).IsValid;
            while (indexFound == false)
            {
                Console.WriteLine($"Index {indexTo} not found. Try again? (Y/N)");
                var userInput = Console.ReadLine();
                if (userInput.SelectedNo())
                {
                    ExitApplication();
                }
                else
                {
                    Console.WriteLine("Do you want to create an index to migrate to? (Y/N)");
                    var userInputTo = Console.ReadLine();
                    if (userInputTo.SelectedNo())
                    {
                        ExitApplication();
                    }
                    else
                    {
                        Console.WriteLine("What do you want to name the new index?");
                        var newIndex = Console.ReadLine();
                        CreateIndex(newIndex);
                    }
                }
                Console.WriteLine("Which index do you want to migrate?");
                var indexFrom = Console.ReadLine();
                indexFound = Client.GetIndex(indexFrom).IsValid;
            }
        }

        private static void CreateIndex(string newIndex)
        {
            if (!string.IsNullOrEmpty(newIndex))
            {
                var createIndex = Client.CreateIndex(newIndex);
                GenerateIndexSuccessfullyCreatedMessage(newIndex, createIndex);
            }
            else
            {
                Console.WriteLine("Index name is invalid, please try again.");
                newIndex = Console.ReadLine();
                var createIndex = Client.CreateIndex(newIndex);
                GenerateIndexSuccessfullyCreatedMessage(newIndex, createIndex);
            }
        }

        private static void FindFromIndex(string indexFrom)
        {
            var indexFound = Client.GetIndex(indexFrom).IsValid;
            while (indexFound == false)
            {
                Console.WriteLine($"Index {indexFrom} not found. Try again? (Y/N)");
                var userInput = Console.ReadLine();
                if (userInput.SelectedNo())
                {
                    ExitApplication();
                }
                Console.WriteLine("Which index do you want to migrate?");
                indexFrom = Console.ReadLine();
                indexFound = Client.GetIndex(indexFrom).IsValid;
            }
        }

        private static void CheckClusterHealth()
        {
            var clusterHealth = Client.ClusterHealthAsync().Result.Status == ClusterStatus.Green.GetType().Name.ToLowerInvariant();
            if (clusterHealth) return;
            Console.WriteLine("Cluster health is not green, do you want to continue? (Y/N)");
            if (Console.ReadLine().SelectedYes())
            {
                Console.WriteLine("This may cause problems. Continuing...");
            }
            else
            {
                ExitApplication();
            }
        }

        private static void BeginMigration()
        {
            Console.WriteLine("Are you ready to begin the index migration? (Y/N)");
            if (Console.ReadLine().SelectedYes())
            {
                Console.WriteLine("Good, let's get started...");
                CheckClusterHealth();
            }
            else
            {
                ExitApplication();
            }
        }

        private static void ExitApplication()
        {
            Console.WriteLine("Exiting application now...");
            Environment.Exit(0);
        }

        private static void GenerateIndexSuccessfullyCreatedMessage(string newIndex, ICreateIndexResponse createIndex)
        {
            Console.WriteLine(createIndex.IsValid
                ? $"Index \"{newIndex}\" created!"
                : $"ERROR: Index \"{newIndex}\" creation failed!");
        }
    }
}
