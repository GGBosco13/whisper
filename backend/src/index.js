require('dotenv').config();
const express = require('express');
const cors = require('cors');
const http = require('http');
const { Server } = require('socket.io');
const redis = require('redis');
const { v4: uuidv4 } = require('uuid');
const bcrypt = require('bcryptjs');
const path = require('path');
const fs = require('fs');

// ============================================================
// WHISPERWEB - Backend Server
// Privacy-first anonymous voice consultation platform
// NO PII is stored: no IP addresses, names, phone numbers
// All session data is ephemeral with TTL in Redis
// ============================================================

const app = express();
const server = http.createServer(app);

// Parse JSON bodies
app.use(express.json());

// CORS: Allow frontend origin only
app.use(cors({
  origin: process.env.FRONTEND_URL || '*',
  credentials: true,
}));

// ============================================================
// REDIS CLIENT — Ephemeral storage with TTL
// CRITICAL: No call recordings, transcripts, or PII stored.
// All keys have auto-expire (TTL) to prevent data accumulation.
// ============================================================
let redisClient;
let redisReady = false;

async function connectRedis() {
  try {
    redisClient = redis.createClient({
      url: process.env.REDIS_URL || `redis://${process.env.REDIS_HOST || 'localhost'}:${process.env.REDIS_PORT || 6379}`,
      socket: { connectTimeout: 2000 },
    });

    await Promise.race([
      redisClient.connect(),
      new Promise((_, reject) =>
        setTimeout(() => reject(new Error('Connection timeout')), 3000)
      ),
    ]);

    redisClient.on('error', () => {});
    redisReady = true;
    console.log('Connected to Redis (ephemeral store)');
  } catch (err) {
    console.warn('Redis unavailable — running in-memory mode');
    redisClient = null;
  }
}

// Ephemeral Redis helper: set with TTL in seconds
async function redisSet(key, value, ttlSeconds = 3600) {
  if (redisClient) {
    await redisClient.setEx(key, ttlSeconds, value);
  } else {
    if (!global._ephemeralStore) global._ephemeralStore = {};
    global._ephemeralStore[key] = value;
  }
}

async function redisGet(key) {
  if (redisClient) {
    return await redisClient.get(key);
  } else {
    return global._ephemeralStore?.[key] || null;
  }
}

async function redisDel(key) {
  if (redisClient) {
    await redisClient.del(key);
  } else {
    if (global._ephemeralStore) delete global._ephemeralStore[key];
  }
}

// ============================================================
// STAFF AUTH ROUTES
// ============================================================
const SUPER_ADMIN_ID = process.env.SUPER_ADMIN_ID || 'whisper-admin';
const SUPER_ADMIN_PASSWORD = process.env.SUPER_ADMIN_PASSWORD || 'whisper-admin-2024';

const staffAccounts = new Map();

async function initAdmin() {
  const hashedPassword = await bcrypt.hash(SUPER_ADMIN_PASSWORD, 10);
  staffAccounts.set(SUPER_ADMIN_ID, {
    id: SUPER_ADMIN_ID,
    passwordHash: hashedPassword,
    role: 'super-admin',
    name: 'System Administrator',
  });
  console.log(`Super admin initialized: ${SUPER_ADMIN_ID}`);
}

// POST /api/auth/login — Staff login
app.post('/api/auth/login', async (req, res) => {
  const { employeeId, password } = req.body;
  if (!employeeId || !password) {
    return res.status(400).json({ error: 'Employee ID and password required' });
  }

  const account = staffAccounts.get(employeeId);
  if (!account) {
    return res.status(401).json({ error: 'Invalid credentials' });
  }

  const valid = await bcrypt.compare(password, account.passwordHash);
  if (!valid) {
    return res.status(401).json({ error: 'Invalid credentials' });
  }

  const token = uuidv4();
  await redisSet(`session:${token}`, employeeId, 86400);

  res.json({ token, employeeId, role: account.role });
});

// POST /api/admin/create-staff
app.post('/api/admin/create-staff', async (req, res) => {
  const { authorization } = req.headers;
  if (!authorization) {
    return res.status(401).json({ error: 'Admin authentication required' });
  }

  const account = staffAccounts.get(authorization);
  if (!account || account.role !== 'super-admin') {
    return res.status(403).json({ error: 'Admin privileges required' });
  }

  const { employeeId, name, password } = req.body;
  if (!employeeId || !name || !password) {
    return res.status(400).json({ error: 'Employee ID, name, and password required' });
  }

  if (staffAccounts.has(employeeId)) {
    return res.status(409).json({ error: 'Employee ID already exists' });
  }

  const hashedPassword = await bcrypt.hash(password, 10);
  staffAccounts.set(employeeId, {
    id: employeeId,
    passwordHash: hashedPassword,
    role: 'staff',
    name,
  });

  res.status(201).json({
    message: 'Staff account created',
    employeeId,
    initialPassword: password,
  });
});

// GET /api/staff/status
app.get('/api/staff/status', async (req, res) => {
  const availableCount = [...staffOnline.values()].filter(
    (s) => s.status === 'available'
  ).length;
  res.json({ availableCount, queueLength: callQueue.length });
});

// ============================================================
// SOCKET.IO — Real-time signaling
// ============================================================
const io = new Server(server, {
  cors: {
    origin: process.env.FRONTEND_URL || '*',
    methods: ['GET', 'POST'],
    credentials: true,
  },
  pingTimeout: 60000,
});

const staffRooms = {};
const staffOnline = new Map();
const callQueue = [];

function findAvailableStaff() {
  const available = [...staffOnline.entries()]
    .filter(([, info]) => info.status === 'available')
    .map(([id, info]) => ({ id, ...info }));

  if (available.length === 0) return null;

  let leastBusy = available[0];
  for (const s of available) {
    const myRooms = (staffOnline.get(s.id)?.roomIds || []).length;
    const leastRooms = (staffOnline.get(leastBusy.id)?.roomIds || []).length;
    if (myRooms < leastRooms) {
      leastBusy = s;
    }
  }
  return leastBusy;
}

io.on('connection', (socket) => {
  socket.on('caller:join', async () => {
    const callerId = uuidv4();
    await redisSet(`caller:${callerId}`, JSON.stringify({
      socketId: socket.id,
      joinedAt: Date.now(),
    }), 3600);

    socket.data.role = 'caller';
    socket.data.callerId = callerId;
    socket.emit('caller:session', { callerId });
    callQueue.push({ callerId, socketId: socket.id, timestamp: Date.now() });
    tryRouteCall();
  });

  socket.on('caller:start-call', () => {
    if (socket.data.role !== 'caller') return;
    if (!callQueue.find((c) => c.callerId === socket.data.callerId)) {
      callQueue.push({
        callerId: socket.data.callerId,
        socketId: socket.id,
        timestamp: Date.now(),
      });
    }
    tryRouteCall();
  });

  socket.on('staff:connect', async ({ employeeId, token }) => {
    const sessionEmployeeId = await redisGet(`session:${token}`);
    if (!sessionEmployeeId) {
      socket.emit('staff:error', { message: 'Invalid session token' });
      socket.disconnect();
      return;
    }

    const account = staffAccounts.get(sessionEmployeeId);
    if (!account || account.role !== 'staff') {
      socket.emit('staff:error', { message: 'Staff privileges required' });
      socket.disconnect();
      return;
    }

    socket.data.role = 'staff';
    socket.data.employeeId = employeeId;
    socket.data.staffName = account.name;

    staffOnline.set(employeeId, {
      socketId: socket.id,
      status: 'available',
      roomIds: [],
      name: account.name,
    });

    socket.emit('staff:connected', { employeeId, name: account.name });
    tryRouteCall();
  });

  socket.on('staff:status-toggle', async ({ status }) => {
    if (socket.data.role !== 'staff') return;
    const info = staffOnline.get(socket.data.employeeId);
    if (info) {
      info.status = status;
      io.emit('staff:availability', {
        availableCount: [...staffOnline.values()].filter((s) => s.status === 'available').length,
      });
      if (status === 'available') tryRouteCall();
    }
  });

  socket.on('staff:accept-call', async ({ callerId, roomId }) => {
    if (socket.data.role !== 'staff') return;

    const callerData = await redisGet(`caller:${callerId}`);
    if (!callerData) {
      socket.emit('call:error', { message: 'Caller session expired' });
      return;
    }

    const callerParsed = JSON.parse(callerData);
    const callerSocket = io.sockets.sockets.get(callerParsed.socketId);
    if (!callerSocket) {
      socket.emit('call:error', { message: 'Caller disconnected' });
      return;
    }

    const room = {
      callerId,
      callerSocketId: callerParsed.socketId,
      staffId: socket.data.employeeId,
      staffSocketId: socket.id,
      startTime: Date.now(),
    };
    staffRooms[roomId] = room;

    const info = staffOnline.get(socket.data.employeeId);
    if (info && !info.roomIds.includes(roomId)) info.roomIds.push(roomId);

    const queueIndex = callQueue.findIndex((c) => c.callerId === callerId);
    if (queueIndex !== -1) callQueue.splice(queueIndex, 1);

    callerSocket.emit('call:connected', { roomId, staffName: info.name });
    socket.emit('call:connected', { roomId, callerId });
  });

  socket.on('staff:decline-call', async ({ callerId }) => {
    if (socket.data.role !== 'staff') return;
    const queueIndex = callQueue.findIndex((c) => c.callerId === callerId);
    if (queueIndex !== -1) callQueue.splice(queueIndex, 1);

    const callerData = await redisGet(`caller:${callerId}`);
    if (callerData) {
      const callerParsed = JSON.parse(callerData);
      io.sockets.sockets.get(callerParsed.socketId)?.emit('call:declined');
    }
  });

  socket.on('webrtc:sdp-offer', ({ roomId, sdp }) => {
    const room = staffRooms[roomId];
    if (!room) return;
    io.sockets.sockets.get(room.staffSocketId)?.emit('webrtc:sdp-offer', { roomId, sdp });
  });

  socket.on('webrtc:sdp-answer', ({ roomId, sdp }) => {
    const room = staffRooms[roomId];
    if (!room) return;
    io.sockets.sockets.get(room.callerSocketId)?.emit('webrtc:sdp-answer', { roomId, sdp });
  });

  socket.on('webrtc:ice-candidate', ({ roomId, candidate }) => {
    const room = staffRooms[roomId];
    if (!room) return;
    if (socket.id === room.callerSocketId) {
      io.sockets.sockets.get(room.staffSocketId)?.emit('webrtc:ice-candidate', { roomId, candidate });
    } else {
      io.sockets.sockets.get(room.callerSocketId)?.emit('webrtc:ice-candidate', { roomId, candidate });
    }
  });

  socket.on('webrtc:config-request', () => {
    const stunServers = process.env.STUN_SERVERS ? JSON.parse(process.env.STUN_SERVERS) : ['stun:stun.l.google.com:19302'];
    const turnServers = process.env.TURN_SERVERS ? JSON.parse(process.env.TURN_SERVERS) : [];
    socket.emit('webrtc:config', { iceServers: [...stunServers, ...turnServers] });
  });

  socket.on('call:end', async ({ roomId }) => {
    const room = staffRooms[roomId];
    if (!room) return;
    io.sockets.sockets.get(room.callerSocketId)?.emit('call:ended', { roomId });
    io.sockets.sockets.get(room.staffSocketId)?.emit('call:ended', { roomId });
    delete staffRooms[roomId];
    const info = staffOnline.get(room.staffId);
    if (info) info.roomIds = (info.roomIds || []).filter((r) => r !== roomId);
  });

  socket.on('disconnect', async (reason) => {
    if (socket.data.role === 'caller') {
      const qi = callQueue.findIndex((c) => c.socketId === socket.id);
      if (qi !== -1) callQueue.splice(qi, 1);

      for (const [rid, room] of Object.entries(staffRooms)) {
        if (room.callerSocketId === socket.id) {
          io.sockets.sockets.get(room.staffSocketId)?.emit('call:ended', { roomId: rid });
          delete staffRooms[rid];
          break;
        }
      }
      if (socket.data.callerId) await redisDel(`caller:${socket.data.callerId}`);
    }

    if (socket.data.role === 'staff') {
      const info = staffOnline.get(socket.data.employeeId);
      if (info) {
        for (const rid of (info.roomIds || [])) {
          const room = staffRooms[rid];
          if (room) {
            io.sockets.sockets.get(room.callerSocketId)?.emit('call:ended', { roomId: rid });
            delete staffRooms[rid];
          }
        }
        delete staffOnline.get(socket.data.employeeId);
      }
    }
  });
});

function tryRouteCall() {
  if (callQueue.length === 0) return;

  let routed = true;
  while (routed && callQueue.length > 0) {
    const caller = callQueue[0];
    const staffInfo = findAvailableStaff();

    if (!staffInfo) {
      routed = false;
      const callerSocket = io.sockets.sockets.get(caller.socketId);
      if (callerSocket) {
        callerSocket.emit('call:no-agents', { message: 'No agents available. Please try again.' });
      }
      break;
    }

    const roomId = `room-${uuidv4().substring(0, 8)}`;
    staffRooms[roomId] = {
      callerId: caller.callerId,
      callerSocketId: caller.socketId,
      staffId: staffInfo.id,
      staffSocketId: staffInfo.socketId,
      startTime: Date.now(),
    };

    callQueue.shift();
    io.sockets.sockets.get(staffInfo.socketId)?.emit('call:incoming', { callerId: caller.callerId, roomId });
    io.sockets.sockets.get(caller.socketId)?.emit('call:ringing', { roomId });
  }
}

// ============================================================
// SERVE SPA
// ============================================================
const STATIC_DIR = path.join(__dirname, '..', '..', 'frontend', 'dist');
app.use(express.static(STATIC_DIR));

app.get('*', (req, res) => {
  const indexPath = path.join(STATIC_DIR, 'index.html');
  if (fs.existsSync(indexPath)) {
    res.sendFile(indexPath);
  } else {
    res.status(404).send('Build not found');
  }
});

// ============================================================
// START SERVER
// ============================================================
async function start() {
  await connectRedis();
  await initAdmin();

  const PORT = process.env.PORT || 3001;
  server.listen(PORT, '0.0.0.0', () => {
    console.log(`WhisperWeb running on port ${PORT}`);
    console.log(`Privacy: No PII stored, all sessions ephemeral`);
    console.log(`STUN: ${process.env.STUN_SERVERS ? 'Configured' : 'Google public STUN only'}`);
  });
}

start().catch(console.error);

module.exports = { app, server, io, staffOnline, staffRooms };
