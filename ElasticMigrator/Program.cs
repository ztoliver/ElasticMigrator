using System;
using Nest;

namespace ElasticMigrator
{
    class Program
    {
        static readonly ElasticClient Client = new ElasticClient(new Uri("http://zacharytoliver.com:9200"));
        private static bool Exit;
        private static string _indexFrom;
        private static string _indexTo;
        static void Main(string[] args)
        {
                BeginMigration();
                CheckClusterHealth();
                MigrateIndex();
        }

        private static void MigrateIndex()
        {
            Console.WriteLine("Which index do you want to migrate?");
            _indexFrom = Console.ReadLine();
            FindFromIndex(_indexFrom);
            Console.WriteLine("Which index do you want to migrate " + _indexFrom + " to?");
            var indexTo = Console.ReadLine();
            FindOrCreateToIndex(indexTo);
            var indexFromDocuments = Client.Search<object>(s => s.Index(_indexFrom).AllTypes().Query(q => q.MatchAll())).Documents;
            foreach (var document in indexFromDocuments)
            {
                Client.Index(document, i => i.Index(indexTo));
            }
        }

        private static void FindOrCreateToIndex(string indexTo)
        {
            var indexFound = Client.GetIndex(indexTo).IsValid;
            if (indexFound)
            {
                _indexTo = indexTo;
            }
            else
            {
                while (indexFound == false)
                {
                    Console.WriteLine("Index " + indexTo + " not found. Try again? (Y/N)");
                    var userInput = Console.ReadLine();
                    if (userInput != "Y")
                    {
                        Environment.Exit(0);
                    }
                    else
                    {
                        Console.WriteLine("Do you want to create an index to migrate to? (Y/N)");
                        var userInputTo = Console.ReadLine();
                        if (userInputTo != "Y")
                        {
                            Environment.Exit(0);
                        }
                        else
                        {
                            Console.WriteLine("What do you want to name the new index?");
                            var newIndex = Console.ReadLine();
                            CreateIndex(newIndex);
                        }
                    }
                    Console.WriteLine("Which index do you want to migrate?");
                    _indexFrom = Console.ReadLine();
                    indexFound = Client.GetIndex(_indexFrom).IsValid;
                }
            }
        }

        private static void CreateIndex(string newIndex)
        {
            while (true)
            {
                if (!string.IsNullOrEmpty(newIndex))
                {
                    var createIndex = Client.CreateIndex(newIndex);
                    if (createIndex.IsValid)
                    {
                        Console.WriteLine("Index \"" + newIndex + "\" created!");
                    }
                    _indexTo = newIndex;
                }
                else
                {
                    Console.WriteLine("Index name is invalid, please try again.");
                    newIndex = Console.ReadLine();
                    continue;
                }
                break;
            }
        }

        private static void FindFromIndex(string indexFrom)
        {
            var indexFound = Client.GetIndex(indexFrom).IsValid;
            while (indexFound ==  false)
            {
                Console.WriteLine("Index " + indexFrom + " not found. Try again? (Y/N)");
                var userInput = Console.ReadLine();
                if (userInput != "Y")
                {
                    Environment.Exit(0);
                }
                Console.WriteLine("Which index do you want to migrate?");
                _indexFrom = Console.ReadLine();
                indexFound = Client.GetIndex(_indexFrom).IsValid;
            }
        }

        private static void CheckClusterHealth()
        {
            var clusterHealth = Client.ClusterHealth().Status == "green";
            if (clusterHealth) return;
            Console.WriteLine("Cluster health is not green, do you want to continue? (Y/N)");
            var userInput = Console.ReadLine();
            Console.WriteLine(userInput == "Y" ? "This may cause problems...continuing" : "Exiting application now...");
            if (userInput != "Y")
            {
                Environment.Exit(0);
            }
        }

        private static void BeginMigration()
        {
            Console.WriteLine("Are you ready to begin migration? (Y/N)");
            var userInput = Console.ReadLine();
            Console.WriteLine(userInput == "Y" ? "Good, let's get started..." : "Exiting application now...");
        }
    }
}
