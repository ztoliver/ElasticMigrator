using System;

namespace ElasticMigrator
{
    public static class ElasticMigratorExtensions
    {
        public static bool SelectedYes(this string userInput)
        {
            return string.Equals(userInput, "Y", StringComparison.InvariantCultureIgnoreCase);
        }

        public static bool SelectedNo(this string userInput)
        {
            return !SelectedYes(userInput);
        }
    }
}