namespace IndexingHelper
{
    using System;
    using System.Collections.Generic;
    using System.Configuration;
    using System.Linq;
    using System.Threading.Tasks;
    using Microsoft.Azure.Documents;
    using Microsoft.Azure.Documents.Client;

    public class Program
    {
        private static readonly string databaseId = ConfigurationManager.AppSettings["DatabaseId"];
        private static readonly string collectionId = ConfigurationManager.AppSettings["CollectionId"];

        private static readonly string endpointUrl = ConfigurationManager.AppSettings["EndPointUrl"];
        private static readonly string authorizationKey = ConfigurationManager.AppSettings["AuthorizationKey"];
        private static readonly ConnectionPolicy connectionPolicy = new ConnectionPolicy { UserAgentSuffix = "Halo queries" };

        private static DocumentClient client;

        public static void Main(string[] args)
        {
            try
            {
                using (client = new DocumentClient(new Uri(endpointUrl), authorizationKey))
                {

                    List<string> idPaths = new List<string>();
                    idPaths.Add(ConfigurationManager.AppSettings["Idpath1"]);
                    idPaths.Add(ConfigurationManager.AppSettings["Idpath2"]);
                    idPaths.Add(ConfigurationManager.AppSettings["Idpath3"]);
                    idPaths.Add(ConfigurationManager.AppSettings["Idpath4"]);

                    AddNewIndexTerms(idPaths).Wait();

                }
            }
            catch (Exception e)
            {
                LogException(e);
            }
            finally
            {
                Console.WriteLine("\npress any key to exit.");
                Console.ReadKey();
            }
        }

        private static async Task PrintIndexingProgress(string db, string coll)
        {
            var collectionUri = UriFactory.CreateDocumentCollectionUri(db, coll);

            ResourceResponse<DocumentCollection> collection = await client.ReadDocumentCollectionAsync(collectionUri);

            Console.WriteLine("Collection index progress " + collection.IndexTransformationProgress);
        }
        private static async Task UpdateIndexingPolicy(string db, string coll)
        {
            var collectionUri = UriFactory.CreateDocumentCollectionUri(db, coll);

            DocumentCollection collection = await client.ReadDocumentCollectionAsync(collectionUri);

            IndexingPolicy indexingPolicy = new IndexingPolicy();
            indexingPolicy.IncludedPaths.Add(new IncludedPath { Path = "/*" });
            collection.IndexingPolicy = indexingPolicy;
            await client.ReplaceDocumentCollectionAsync(collection);
        }
        private static async Task AddNewIndexTerm(string newPath)
        {
            var collectionUri = UriFactory.CreateDocumentCollectionUri(databaseId, collectionId);


            Console.WriteLine("\n1. Reading document collection for Database:{0} - Collection:{1}", databaseId, collectionId);
            DocumentCollection collection = await client.ReadDocumentCollectionAsync(collectionUri);

            Console.WriteLine("Collection {0} with index policy \n{1}", collection.Id, collection.IndexingPolicy);

            Console.WriteLine("Adding new path {0} to Collection {1}", collection.Id, newPath);

            collection.IndexingPolicy.IncludedPaths.Add(new IncludedPath { Path = newPath });
            await client.ReplaceDocumentCollectionAsync(collection);

        }

        private static async Task AddNewIndexTerms(List<string> newPaths)
        {
            var collectionUri = UriFactory.CreateDocumentCollectionUri(databaseId, collectionId);


            Console.WriteLine("\n1. Reading document collection for Database:{0} - Collection:{1}", databaseId, collectionId);
            DocumentCollection collection = await client.ReadDocumentCollectionAsync(collectionUri);

            Console.WriteLine("Collection {0} with index policy \n{1}", collection.Id, collection.IndexingPolicy);

            foreach (string s in newPaths)
            {
                Console.WriteLine("Adding new path {0} to Collection {1}", collection.Id, s);

                collection.IndexingPolicy.IncludedPaths.Add(new IncludedPath { Path = s });
            }
            await client.ReplaceDocumentCollectionAsync(collection);

        }

        private static async Task AddExcludeNewIndexTerm(string newPath)
        {
            var collectionUri = UriFactory.CreateDocumentCollectionUri(databaseId, collectionId);


            Console.WriteLine("\n1. Reading document collection for Database:{0} - Collection:{1}", databaseId, collectionId);
            DocumentCollection collection = await client.ReadDocumentCollectionAsync(collectionUri);

            Console.WriteLine("Collection {0} with index policy \n{1}", collection.Id, collection.IndexingPolicy);

            Console.WriteLine("Adding new path {0} to Collection {1}", collection.Id, newPath);

            collection.IndexingPolicy.ExcludedPaths.Add(new ExcludedPath { Path = newPath });
            await client.ReplaceDocumentCollectionAsync(collection);

        }

        private static async Task UpdateDatabaseRus()
        {
            var databaseUri = UriFactory.CreateDatabaseUri(databaseId);

            Console.WriteLine("\nReading document collection for Database:{0} - Collection:{1}", databaseId, collectionId);
            Database database = await client.ReadDatabaseAsync(databaseUri);
            Offer offer = client.CreateOfferQuery().Where(o => o.ResourceLink == database.SelfLink).AsEnumerable().Single();

            Offer replaced = await client.ReplaceOfferAsync(new OfferV2(offer, 10000));
            Console.WriteLine("\nReplaced Offer. Offer is now {0}.\n", replaced);

            // Get the offer again after replace
            offer = client.CreateOfferQuery().Where(o => o.ResourceLink == database.SelfLink).AsEnumerable().Single();
            OfferV2 offerV2 = (OfferV2)offer;
            Console.WriteLine(offerV2.Content.OfferThroughput);

            Console.WriteLine(
                "Rechecked the updated Offer \n{0}\n using collection's ResourceId {1}.\n",
                offer,
                database.ResourceId);

        }
        private static async Task<DocumentCollection> CreateCollectionRus(
            string databaseName,
            string collectionName,
            int rus,
            string partitionKey)
        {
            DocumentCollection collectionDefinition = new DocumentCollection();
            collectionDefinition.Id = collectionName;
            collectionDefinition.PartitionKey.Paths.Add(partitionKey);

            DocumentCollection partitionedCollection = await client.CreateDocumentCollectionIfNotExistsAsync(
                UriFactory.CreateDatabaseUri(databaseName),
                collectionDefinition,
                new RequestOptions { OfferThroughput = rus });

            Console.WriteLine("\n1.1. Created Collection \n{0}", partitionedCollection);

            return partitionedCollection;


        }

        private static async Task UpdateCollectionRus()
        {
            var collectionUri = UriFactory.CreateDocumentCollectionUri(databaseId, collectionId);

            Console.WriteLine("\nReading document collection for Database:{0} - Collection:{1}", databaseId, collectionId);
            DocumentCollection collection = await client.ReadDocumentCollectionAsync(collectionUri);
            Offer offer = client.CreateOfferQuery().Where(o => o.ResourceLink == collection.SelfLink).AsEnumerable().Single();

            Offer replaced = await client.ReplaceOfferAsync(new OfferV2(offer, 10000));
            Console.WriteLine("\nReplaced Offer. Offer is now {0}.\n", replaced);

            // Get the offer again after replace
            offer = client.CreateOfferQuery().Where(o => o.ResourceLink == collection.SelfLink).AsEnumerable().Single();
            OfferV2 offerV2 = (OfferV2)offer;
            Console.WriteLine(offerV2.Content.OfferThroughput);

            Console.WriteLine(
                "Rechecked the updated Offer \n{0}\n using collection's ResourceId {1}.\n",
                offer,
                collection.ResourceId);


        }


        /// <summary>
        /// Log exception error message to the console
        /// </summary>
        /// <param name="e">The caught exception.</param>
        private static void LogException(Exception e)
        {
            ConsoleColor color = Console.ForegroundColor;
            Console.ForegroundColor = ConsoleColor.Red;

            Exception baseException = e.GetBaseException();
            if (e is DocumentClientException)
            {
                DocumentClientException de = (DocumentClientException)e;
                Console.WriteLine("{0} error occurred: {1}, Message: {2}", de.StatusCode, de.Message, baseException.Message);
            }
            else
            {
                Console.WriteLine("Error: {0}, Message: {1}", e.Message, baseException.Message);
            }

            Console.ForegroundColor = color;
        }
    }
}
