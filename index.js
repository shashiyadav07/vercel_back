import express from 'express'
import connection from './config/dbconfig.js'
import http from 'http'
import { Server } from 'socket.io'
import cors from "cors";
import multer from 'multer'
import path from 'path'
import dotenv from "dotenv";
dotenv.config();
// import upload from './config/multer.js';
import {v2 as cloudinary} from 'cloudinary'
import { Socket } from 'dgram';

cloudinary.config({
  cloud_name: process.env.CLOUDINARY_CLOUD_NAME,
  api_key:process.env.CLOUDINARY_API_KEY,
  api_secret: process.env.CLOUDINARY_API_SECRET,
})
// export default cloudinary


const port = process.env.PORT || 4000;

const app = express();


app.use(cors({
  origin: "https://vercel-front-opal-seven.vercel.app",
  credentials: true
}));

app.use(express.json());


const db = await connection()
const collection = db.collection('login')
const messageCollection = db.collection('message')
const contactCollection = db.collection('contact')
const follow_statusCollection = db.collection('followStatus')

const storage = multer.diskStorage({
  // destination:(req,file,cb)=>{
  //   cb(null,'./upload')
  // },
  filename:(req,file,cb)=>{
    cb(null,Date.now()+path.extname(file.originalname))
  }
})
const upload = multer({storage:storage})

app.post('/signup',upload.single("profilePic"), async (req, res) => {
  try {
    const { name, email, password } = req.body
    console.log(req.file)
    const file = req.file.path
    const cloudinaryResponse = await cloudinary.uploader.upload(file, {
      folder: '/upload_file',
      // unique_filename: true,
      use_filename: true,
    })
    console.log('Cloudinary response:', cloudinaryResponse.secure_url)
    const profilePicUrl = cloudinaryResponse.secure_url

    const user = await collection.findOne({ email })

    if (user) {
      console.log('already exists')
      return res.json({
        success: false,
        message: 'Email already exists'
      })
    }

    await collection.insertOne({
      name,
      email,
      password,
      profilePic: profilePicUrl
    })

    res.json({
      email: email,
      profilePic: profilePicUrl,
      success: true,
      message: 'Signup successful'
    })
  } catch (err) {
    console.error('Signup error:', err)
    res.status(500).json({
      success: false,
      message: err.message
    })
  }
})

app.post('/', async (req, res) => {
  try {
    const { email, password } = req.body;

    const user = await collection.findOne({ email, password });

    if (user === null) {
      return res.json({
        success: false,
        message: "Login data incorrect"
      });
    }

    res.json({
      _id: user._id,
      name: user.name,
      profilePic: user.profilePic,
      success: true,
      message: "Login successful"
    });

  } catch (err) {
    console.error('login error:', err);
    res.status(500).json({
      success: false,
      message: 'Server error'
    });
  }
});
// app.get('/', async (req,res)=>{
//   const {email , password} = req.body
//   const user = await collection.findOne({email,password})
//   if(user){
//     return res.json({
//       name: user.name
//     })
//   }
// })

const server = http.createServer(app)
const io = new Server(server,{
  cors:{
    origin:"https://vercel-front-opal-seven.vercel.app",
    methods:["GET","POST"],
    credentials:true
  }
})

let connectedClients = 0
const onlineUser = new Map()
io.on('connection', (socket) => {

socket.on("createRoom", ({ sender_id, reciver_id }) => {
console.log("sender Socket connected:", socket.id);
    const roomId = [sender_id, reciver_id].sort().join("-");

    socket.join(roomId);

    socket.emit("roomCreated", {
      roomId,
    });

    console.log(`${socket.id} joined ${roomId}`);
  });

  console.log('connected :', socket.id)
  socket.on("userData",(userInfo)=>{
    onlineUser.set(socket.id,{
      profilePic: userInfo.profilePic,
      userId: userInfo._id,
      username : userInfo.username,
      socketId : socket.id
    })
    io.emit('online-user',[...onlineUser.values()])
     
  })
//  io.on("connection", (socket) => {

 

// });
  // socket.on('privateMessage', (data)=>{
  //   const { sender_id, reciver_id, recevier, sender } = data;
  //   const roomId = [sender_id, reciver_id].sort().join('-');
  //   sender && recevier && socket.join(roomId);
  //   socket.emit('room-created', roomId);
  // });
connectedClients++

io.emit('client',connectedClients);
socket.on('private-msg', async ({ receiverSocketId, sender_id, reciver_id, roomId, message, username, profilePic, createdAt }) => {
  //  console.log("Room:", roomId);
  // console.log("Members:", io.sockets.adapter.rooms.get(roomId));

  // console.log('Private message received:', {
  //   senderSocketId: socket.id,
  //   receiverSocketId,
  //   roomId
  // });
  // console.log('ab mongodb me insert karna hai');
  try{
    const msg = await messageCollection.insertOne({
  roomId,
  username,
  sender_id,
  reciver_id,
  profilePic,
  message,
  senderSocketId: socket.id,
  receiverSocketId,
  createdAt
});
console.log('message inserted data', msg)
  }
  catch(err){
    console.error('Error inserting message into database:', err);
  }
  // 
  // socket.emit('chatData', chatData)
  socket.to(roomId).emit("recive-msg", {
  senderSocketId: socket.id,
  sender_id: sender_id,
  reciver_id: reciver_id,
  receiverSocketId,
  roomId,
  message,
  username,
  profilePic,
  createdAt
});
});
socket.on("chatData", async ({ roomId }) => {
  const chatData = await messageCollection
    .find({ roomId })
    .toArray();

  socket.emit("chatData", {
    roomId,
    messages: chatData,
  });
});
socket.on('contact_request', (data)=>{
  // console.log('contact request received in backend:', data)
  io.to(data.socketId).emit('contact_request', data)
//   console.log("Receiver SocketId:", data.socketId);
// console.log("Socket Exists:", io.sockets.sockets.has(data.socketId))
})
socket.on('Accept_request',(data)=>{
  console.log("Accept_request:",data)
  io.to(data.socketId).emit('Accept_request', 
     {
       userId: data.sender_id,//jo req accept kar raha hai
      reciver_id: data.reciver_id,// jiska req accept ho raha hai
      username: data.sender_name,//jo req accept kar raha hai
      profilePic: data.profilePic,//jo req accept kar raha hai
      // reciver_name: data.reciver_name,// jiska req accept ho raha hai
      // socketId: data.socketId,//jiska req accept ho raha hai
      socketId: socket.id
    }
  )
})
socket.on('follow_status', async (data)=>{
 await follow_statusCollection.insertOne({
  owner_id: data.owner_id,
  follow_id: data.follow_id,
  status : data.status
 })
})

socket.on("accept_status", async (data) => {
  await follow_statusCollection.updateOne(
    {
      owner_id: data.owner_id,
      follow_id: data.accept_for,
    },
    {
      $set: {
        status: data.status
      }
    }
  );
});
socket.on('unfollow', async (data)=>{
  await follow_statusCollection.updateOne(
    {
      owner_id: data.owner_id,
      follow_id:data.unfollow_id,
    },
    {
      $set:{
        status: data.status
      }
    }
  )
})
 socket.on("follow_accept", async (data) => {
  console.log("follow_accept data:", data);

  const result = await follow_statusCollection.updateOne(
    {
      owner_id: data.owner_id,
      follow_id: data.accept_by,
    },
    {
      $set: {
        status: data.status,
      },
    }
  );

  console.log(result);
});
  socket.on('accept_follow_request',async (data)=>{
    console.log(data)
    await follow_statusCollection.insertOne({
      owner_id: data.owner_id,
      follow_id:data.reciver ,
      status : data.status
    })
  })
socket.on("followData", async (ownerId) => {
  const followData = await follow_statusCollection
    .find({ owner_id: ownerId })
    .toArray();

  socket.emit("followData", followData);
});

socket.on('saveContact', async (data)=>{
  await contactCollection.insertOne({
    owner_id: data.owner_id,
    profilePic: data.contact.profilePic,
    userId: data.contact.userId,
    username: data.contact.username,
    socketId: data.contact.socketId
})})
socket.on('saveContact2',async (data)=>{
   await contactCollection.insertOne({
    owner_id: data.owner_id,
    profilePic: data.contact.profilePic,
    userId: data.contact.userId,
    username: data.contact.username,
    socketId: data.contact.socketId
})
})
socket.on("contactData", async (ownerId) => {
  const contacts = await contactCollection
    .find({ owner_id: ownerId })
    .toArray();

  socket.emit("contactData", contacts);
});
socket.on('delete_data',async (data)=>{
  await contactCollection.deleteOne({ owner_id : data.owner_id,userId : data.userId})
})
socket.on('unfollow_request',async (data)=>{
  const unfollow = await contactCollection.deleteOne({owner_id:data.sender_id, userId : data.reciver_id})
   io.to(data.socketId).emit('unfollow_request', data)
})
socket.on('disconnect',()=>{
  onlineUser.delete(socket.id)
   io.emit('online-user',[...onlineUser.values()])
  connectedClients--
  io.emit('client',connectedClients)
})
  // socket.on('signup', async (data) => {
  //   try {
  //     // console.log('signup data:', data)

  //     const user = await collection.findOne({ email: data.email })
  //     if (user != null) {
  //       socket.emit('signupRes', {
  //         success: false,
  //         message: data.email + ' already exists'
  //       })
  //       // console.log('user already exists')
  //       return
  //     }

  //     await collection.insertOne({
  //       name: data.name,
  //       email: data.email,
  //       password: data.password
  //     })

  //     socket.emit('signupRes', {
  //       success: true,
  //       message: 'Signup successful'
  //     })
  //   } catch (err) {
  //     console.error('Signup error:', err)
  //     socket.emit('signupRes', {
  //       success: false,
  //       message: 'Signup failed'
  //     })
  //   }
  // })
  // socket.on("loginData",  async (data)=>{
  //   // console.log(data)
  //   try{
  //     const check = await collection.findOne({email:data.email,password:data.password})
  //     if(check != null){
  //       // console.log('welcome');
  //       socket.emit('loginRes',{
  //         success:true,
  //         message:"welcome"
  //       })
  //     }else{
  //       // console.log('do signup');
  //       socket.emit('loginRes',{
  //         success:false,
  //         message:"do signup"
  //       })
  //     }
  //   }
  //   catch(err){
  //     console.log('error:internal server');
  //   }
  // })
})

server.listen(port, () => {
  console.log('Server listening on port 4000')
})