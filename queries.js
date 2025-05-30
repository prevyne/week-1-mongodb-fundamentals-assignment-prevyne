// Import MongoDB client
const { MongoClient } = require('mongodb');

// Connection URI (replace with your MongoDB connection string if using Atlas)
const uri = 'mongodb+srv://Prevyne:Justcause2@cluster0.akx03ss.mongodb.net/';

// Database and collection names
const dbName = 'plp_bookstore';
const collectionName = 'books';


// Function to perform filter queries
async function queryOps() {
  const client = new MongoClient(uri);

  try {
    // Connect to the MongoDB server
    await client.connect();
    console.log('Connected to MongoDB server');

    // Get database and collection
    const db = client.db(dbName);
    const collection = db.collection(collectionName);

    //1.  Display the books in specific genres
    const bookByGenre = await collection.aggregate([
      {$match: {genre: "Fantasy"}}
    ]).toArray();
    console.log('Fantasy books: ', bookByGenre);

    //2. Books published in 1960
    const bookByYear = await collection.aggregate([
      {$match: {published_year: 1960}}
    ]).toArray();
    console.log('Books published in 1960: ', bookByYear);

    //3. Display books writtenn by a specific author
    const bookByAuthor = await collection.aggregate([
      {$match: {author: "George Orwell"}}
    ]).toArray();
    console.log('George Orwell\'s works: ', bookByAuthor);

    //4. Update price of a specific book
    const updatedBookPrice = await collection.updateOne(
      {title: "The Alchemist"},
      {$set: {price: 9.50}}
    );
    console.log('Updated "The Alchemist\'s" Price: ', updatedBookPrice.modifiedCount);

    //5. Delete a book by its title
    const deletedBookByTitle = await collection.deleteOne(
      {title: "Moby Dick"}
    );
    console.log('Deleted "Moby Dick": ', deletedBookByTitle.deletedCount);

    //Task 3. ADVANCED QUERRIES
    //1. Write a query to find books that are both in stock and published after 2010
    const inStock_and_Published2010 = await collection.aggregate([
      {$match: {$and: [{in_stock: true},{published_year: 2010}]}}
    ]).toArray();
    console.log('Books in stock and published in 2010: ', inStock_and_Published2010);

    //2. Use projection to return only the title, author, and price fields in your queries
    const projectionResult = await collection.find({}, {title: 1, author: 1, price: 1, _id: 0}).toArray();
    console.log('title, author, and price fields only: ', projectionResult);

    //3. Implement sorting to display books by price (both ascending and descending)
    const booksByPriceAsc = await collection.aggregate([
      {$sort: {price: 1}}
    ]).toArray();
    console.log('Books by price in ascendin order: ', booksByPriceAsc);

    const booksByPriceDesc = await collection.aggregate([
      {$sort: {price: -1}}
    ]).toArray();
    console.log('Books by price in descending order: ', booksByPriceDesc);

    //4. Use the limit and skip methods to implement pagination (5 books per page)
      // Helper function for pagination to make it reusable
      const getPaginatedBooks = async (pageNumber, pageSize) => {
          const skipAmount = (pageNumber - 1) * pageSize;
          console.log(`--- Fetching Page ${pageNumber} (pageSize: ${pageSize}, skip: ${skipAmount}) ---`);

          const paginatedBooks = await collection.find({})
                                                  .skip(skipAmount)
                                                  .limit(pageSize)
                                                  .toArray();
          return paginatedBooks;
      };

      const pageSize = 5; 

      // Get and display page 1
      const page1Books = await getPaginatedBooks(1, pageSize);
      console.log(`Page 1 (${pageSize} books per page): `, page1Books);

      // Get and display page 2
      const page2Books = await getPaginatedBooks(2, pageSize);
      console.log(`Page 2 (${pageSize} books per page): `, page2Books);

      // Get and display page 3 (if enough data exists)
      const page3Books = await getPaginatedBooks(3, pageSize);
      console.log(`Page 3 (${pageSize} books per page): `, page3Books);

    // Task 4: AGGREGATE PIPELINE
    //1. Create an index on the title field for faster searches
    const indexResult = await collection.createIndex({title: 1});
    console.log('Index created on "title field": ', indexResult);

    //2. Create a compound index on author and published_year
    const compoundIndex = await collection.createIndex({author: 1, published_year: 1});
    console.log('compound index crated at "author" and "published_year" fields: ', compoundIndex);
    //Use the explain() method to demonstrate the performance improvement with your indexes
    const explainCompoundQuery = await collection.find({author: "George Orwell"})
                                          .sort({published_year: -1})
                                          .explain('executionStats');
    console.log('\nExplain for query on author and sorted by "published_year": ');
    console.log(JSON.stringify(explainCompoundQuery, null, 2));
    
  } catch (err) {
    console.error('Error occurred:', err);
  } finally {
    // Close the connection
    await client.close();
    console.log('Connection closed');
  }
}

// Run the function
queryOps().catch(console.error);
