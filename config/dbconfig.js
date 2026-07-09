import { MongoClient } from 'mongodb'
import dotenv from "dotenv";
dotenv.config();

const url = process.env.MONGO_URI
const client = new MongoClient(url)
const dbName = 'chatApp'
let isConnected = false

const connection = async () => {
  try {
    if (!isConnected) {
      await client.connect()
      isConnected = true
      console.log('DB connected')
    }

    return client.db(dbName)
  } catch (err) {
    console.error('MongoDB connection error:', err)
    throw err
  }
}

export default connection