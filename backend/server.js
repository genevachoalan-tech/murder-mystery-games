/**
 * 剧本杀联机后端服务器
 * Node.js + Express + Socket.IO
 *
 * 支持三个游戏：guianlu（诡案录）/ fenxinlu（焚心录）/ jinlou（金楼）
 * 内存存储房间状态，免费部署即可使用。
 */

const express = require('express');
const http = require('http');
const { Server } = require('socket.io');
const path = require('path');

// ---------------------------------------------------------------------------
// 配置
// ---------------------------------------------------------------------------
const PORT = process.env.PORT || 3000;
const MAX_PLAYERS_PER_ROOM = 8;          // 每个房间最多 8 人
const ROOM_IDLE_TIMEOUT_MS = 30 * 60 * 1000; // 房间空闲 30 分钟自动清理
const RECONNECT_GRACE_MS = 10 * 60 * 1000;   // 断线后 10 分钟内可重连

// CORS：允许 GitHub Pages 及本地开发环境跨域
const CORS_ORIGINS = [
  'http://localhost:5173',
  'http://localhost:3000',
  'http://127.0.0.1:5173',
  'http://127.0.0.1:3000',
];
// 允许任意 *.github.io 子域（GitHub Pages），也允许通过环境变量追加
if (process.env.ALLOWED_ORIGIN) {
  CORS_ORIGINS.push(process.env.ALLOWED_ORIGIN);
}

// ---------------------------------------------------------------------------
// 应用初始化
// ---------------------------------------------------------------------------
const app = express();
const httpServer = http.createServer(app);

const io = new Server(httpServer, {
  cors: {
    // origin 可以是字符串数组或函数；这里用函数兼容 GitHub Pages 动态子域
    origin: (origin, callback) => {
      // 允许无 origin 的请求（如原生 WebSocket、Postman、curl）
      if (!origin) return callback(null, true);
      // 白名单
      if (CORS_ORIGINS.includes(origin)) return callback(null, true);
      // 允许所有 *.github.io（GitHub Pages）
      if (/^https?:\/\/[\w-]+\.github\.io$/.test(origin)) return callback(null, true);
      // 其他来源拒绝
      return callback(new Error('CORS blocked: ' + origin), false);
    },
    methods: ['GET', 'POST'],
    credentials: true,
  },
});

// ---------------------------------------------------------------------------
// 内存数据结构
// ---------------------------------------------------------------------------

/**
 * rooms: Map<roomCode, Room>
 * Room = {
 *   code: string,              // 6 位数字房间码
 *   hostId: string,            // 房主 playerId
 *   gameType: string,          // guianlu / fenxinlu / jinlou
 *   createdAt: number,         // 创建时间戳
 *   lastActiveAt: number,      // 最后活跃时间戳（用于空闲清理）
 *   gameState: any,            // 游戏状态（任意 JSON，由前端定义）
 *   players: Map<playerId, Player>
 * }
 *
 * Player = {
 *   id: string,
 *   nickname: string,
 *   role: string,              // 角色名
 *   online: boolean,           // 是否在线
 *   socketId: string | null,   // 当前 socket 连接 id
 *   isHost: boolean,
 *   ready: boolean,            // 是否准备就绪
 *   joinedAt: number,
 * }
 */
const rooms = new Map();

// 房间码冲突重试
const MAX_ROOM_CODE_ATTEMPTS = 20;

// ---------------------------------------------------------------------------
// 工具函数
// ---------------------------------------------------------------------------

/** 生成 6 位数字房间码 */
function generateRoomCode() {
  let code;
  let attempts = 0;
  do {
    code = String(Math.floor(100000 + Math.random() * 900000)); // 100000 ~ 999999
    attempts++;
  } while (rooms.has(code) && attempts < MAX_ROOM_CODE_ATTEMPTS);
  if (rooms.has(code)) {
    throw new Error('无法生成唯一房间码，请重试');
  }
  return code;
}

/** 生成玩家 ID（短随机串） */
function generatePlayerId() {
  return 'p_' + Math.random().toString(36).slice(2, 10) + Date.now().toString(36).slice(-4);
}

/** 更新房间最后活跃时间 */
function touchRoom(roomCode) {
  const room = rooms.get(roomCode);
  if (room) room.lastActiveAt = Date.now();
}

/** 将房间内玩家列表序列化为可广播的纯对象 */
function serializePlayers(room) {
  return Array.from(room.players.values()).map((p) => ({
    id: p.id,
    nickname: p.nickname,
    role: p.role,
    online: p.online,
    isHost: p.isHost,
    ready: p.ready,
  }));
}

/** 将房间状态序列化为可广播的纯对象 */
function serializeRoom(room) {
  return {
    roomCode: room.code,
    hostId: room.hostId,
    gameType: room.gameType,
    players: serializePlayers(room),
    gameState: room.gameState,
  };
}

/** 给房间内所有 socket 发系统消息 */
function broadcastSystemMessage(io, roomCode, text) {
  io.to(roomCode).emit('chatMessage', {
    playerId: 'system',
    nickname: '系统',
    message: text,
    timestamp: Date.now(),
    type: 'system',
  });
}

/**
 * 房间空闲自动清理：
 * 每 5 分钟扫描一次，超过 ROOM_IDLE_TIMEOUT_MS 未活跃的房间删除。
 */
setInterval(() => {
  const now = Date.now();
  for (const [code, room] of rooms) {
    if (now - room.lastActiveAt > ROOM_IDLE_TIMEOUT_MS) {
      console.log(`[GC] 房间 ${code} 空闲超时，已清理`);
      io.to(code).emit('roomClosed', { reason: '房间空闲超时，已自动解散' });
      io.in(code).disconnectSockets(true);
      rooms.delete(code);
    }
  }
}, 5 * 60 * 1000);

// ---------------------------------------------------------------------------
// HTTP 路由（健康检查）
// ---------------------------------------------------------------------------
app.get('/', (req, res) => {
  res.json({
    name: '剧本杀联机服务器',
    status: 'running',
    rooms: rooms.size,
    uptime: process.uptime(),
  });
});

app.get('/health', (req, res) => {
  res.status(200).json({ ok: true, rooms: rooms.size });
});

// ---------------------------------------------------------------------------
// Socket.IO 事件处理
// ---------------------------------------------------------------------------
io.on('connection', (socket) => {
  console.log(`[connect] socket ${socket.id} 已连接`);

  /**
   * createRoom { nickname, role, gameType }
   * 创建者自动成为房主。
   */
  socket.on('createRoom', (payload, ack) => {
    try {
      const { nickname, role, gameType } = payload || {};
      if (!nickname || !gameType) {
        throw new Error('缺少 nickname 或 gameType');
      }

      const roomCode = generateRoomCode();
      const playerId = generatePlayerId();

      const room = {
        code: roomCode,
        hostId: playerId,
        gameType,
        createdAt: Date.now(),
        lastActiveAt: Date.now(),
        gameState: {},
        players: new Map(),
      };

      const player = {
        id: playerId,
        nickname,
        role: role || '',
        online: true,
        socketId: socket.id,
        isHost: true,
        ready: false,
        joinedAt: Date.now(),
      };

      room.players.set(playerId, player);
      rooms.set(roomCode, room);

      // socket 加入房间
      socket.join(roomCode);
      socket.data.roomCode = roomCode;
      socket.data.playerId = playerId;

      console.log(`[createRoom] 房间 ${roomCode} 创建，房主 ${nickname}(${playerId})，游戏 ${gameType}`);

      // 返回给创建者
      socket.emit('roomCreated', { roomCode, playerId });
      socket.emit('playerJoined', { player: serializePlayers(room).find(p => p.id === playerId), players: serializePlayers(room) });
      socket.emit('stateUpdated', { gameState: room.gameState, updatedBy: 'system' });

      if (typeof ack === 'function') {
        ack({ success: true, roomCode, playerId });
      }
    } catch (err) {
      console.error('[createRoom] 错误:', err.message);
      socket.emit('error', { message: err.message });
      if (typeof ack === 'function') ack({ success: false, message: err.message });
    }
  });

  /**
   * joinRoom { roomCode, nickname, role, gameType }
   * 通过房间码加入房间。
   */
  socket.on('joinRoom', (payload, ack) => {
    try {
      const { roomCode, nickname, role, gameType } = payload || {};
      if (!roomCode || !nickname) {
        throw new Error('缺少 roomCode 或 nickname');
      }

      const room = rooms.get(roomCode);
      if (!room) {
        throw new Error('房间不存在或已解散');
      }

      // 人数检查（只统计在线玩家）
      const onlineCount = Array.from(room.players.values()).filter(p => p.online).length;
      if (onlineCount >= MAX_PLAYERS_PER_ROOM) {
        throw new Error('房间已满（最多 8 人）');
      }

      // 如果传入 gameType，记录但不强制一致
      if (gameType) room.gameType = gameType;

      // 判断是否是断线重连（相同 playerId 在房间里且之前 offline）
      let player = null;
      let isReconnect = false;
      for (const p of room.players.values()) {
        if (p.nickname === nickname && !p.online) {
          // 允许同昵称重连
          player = p;
          isReconnect = true;
          break;
        }
      }

      if (!player) {
        // 新玩家
        const playerId = generatePlayerId();
        player = {
          id: playerId,
          nickname,
          role: role || '',
          online: true,
          socketId: socket.id,
          isHost: false,
          ready: false,
          joinedAt: Date.now(),
        };
        room.players.set(playerId, player);
      } else {
        // 重连：恢复在线状态
        player.online = true;
        player.socketId = socket.id;
        if (role) player.role = role;
      }

      touchRoom(roomCode);
      socket.join(roomCode);
      socket.data.roomCode = roomCode;
      socket.data.playerId = player.id;

      console.log(`[joinRoom] ${nickname}(${player.id}) 加入房间 ${roomCode}${isReconnect ? '（重连）' : ''}`);

      const players = serializePlayers(room);

      // 给加入者返回完整房间状态
      socket.emit('joinedRoom', {
        success: true,
        roomState: serializeRoom(room),
        players,
        gameState: room.gameState,
        you: player.id,
        isHost: player.isHost,
      });

      // 广播给房间其他人：新玩家加入 / 玩家重新上线
      if (isReconnect) {
        io.to(roomCode).emit('playerStatusChange', { playerId: player.id, online: true });
        broadcastSystemMessage(io, roomCode, `${nickname} 重新连接`);
      } else {
        io.to(roomCode).emit('playerJoined', {
          player: players.find(p => p.id === player.id),
          players,
        });
        broadcastSystemMessage(io, roomCode, `${nickname} 加入了房间`);
      }

      // 始终把完整 gameState 同步给新连接
      socket.emit('stateUpdated', { gameState: room.gameState, updatedBy: player.id });

      if (typeof ack === 'function') {
        ack({ success: true, roomState: serializeRoom(room), players, gameState: room.gameState, you: player.id });
      }
    } catch (err) {
      console.error('[joinRoom] 错误:', err.message);
      socket.emit('error', { message: err.message });
      if (typeof ack === 'function') ack({ success: false, message: err.message });
    }
  });

  /**
   * reconnect { roomCode, playerId }
   * 显式重连：根据 playerId 恢复状态。
   */
  socket.on('reconnect', (payload, ack) => {
    try {
      const { roomCode, playerId } = payload || {};
      if (!roomCode || !playerId) throw new Error('缺少 roomCode 或 playerId');

      const room = rooms.get(roomCode);
      if (!room) throw new Error('房间不存在或已解散');

      const player = room.players.get(playerId);
      if (!player) throw new Error('玩家不在该房间');

      player.online = true;
      player.socketId = socket.id;
      touchRoom(roomCode);

      socket.join(roomCode);
      socket.data.roomCode = roomCode;
      socket.data.playerId = playerId;

      const players = serializePlayers(room);
      console.log(`[reconnect] ${player.nickname}(${playerId}) 重连房间 ${roomCode}`);

      socket.emit('joinedRoom', {
        success: true,
        roomState: serializeRoom(room),
        players,
        gameState: room.gameState,
        you: playerId,
        isHost: player.isHost,
      });
      socket.emit('stateUpdated', { gameState: room.gameState, updatedBy: playerId });
      io.to(roomCode).emit('playerStatusChange', { playerId, online: true });
      broadcastSystemMessage(io, roomCode, `${player.nickname} 重新连接`);

      if (typeof ack === 'function') ack({ success: true, players, gameState: room.gameState });
    } catch (err) {
      console.error('[reconnect] 错误:', err.message);
      socket.emit('error', { message: err.message });
      if (typeof ack === 'function') ack({ success: false, message: err.message });
    }
  });

  /**
   * stateUpdate { roomCode, gameState }
   * 任何玩家可发送，后端更新并广播。
   */
  socket.on('stateUpdate', (payload) => {
    try {
      const { roomCode, gameState } = payload || {};
      if (!roomCode || gameState === undefined) throw new Error('缺少 roomCode 或 gameState');

      const room = rooms.get(roomCode);
      if (!room) throw new Error('房间不存在');

      room.gameState = gameState;
      touchRoom(roomCode);

      const updatedBy = socket.data.playerId || 'unknown';
      io.to(roomCode).emit('stateUpdated', { gameState: room.gameState, updatedBy });
    } catch (err) {
      console.error('[stateUpdate] 错误:', err.message);
      socket.emit('error', { message: err.message });
    }
  });

  /**
   * chatMessage { roomCode, message }
   * 广播聊天消息。
   */
  socket.on('chatMessage', (payload) => {
    try {
      const { roomCode, message } = payload || {};
      if (!roomCode || !message) throw new Error('缺少 roomCode 或 message');

      const room = rooms.get(roomCode);
      if (!room) throw new Error('房间不存在');

      const playerId = socket.data.playerId;
      const player = playerId ? room.players.get(playerId) : null;
      touchRoom(roomCode);

      io.to(roomCode).emit('chatMessage', {
        playerId: playerId || 'unknown',
        nickname: player ? player.nickname : '未知玩家',
        message,
        timestamp: Date.now(),
        type: 'text',
      });
    } catch (err) {
      console.error('[chatMessage] 错误:', err.message);
      socket.emit('error', { message: err.message });
    }
  });

  /**
   * triggerJumpscare { roomCode, type }
   * 全房间同时触发 Jump Scare。
   */
  socket.on('triggerJumpscare', (payload) => {
    try {
      const { roomCode, type } = payload || {};
      if (!roomCode || !type) throw new Error('缺少 roomCode 或 type');

      const room = rooms.get(roomCode);
      if (!room) throw new Error('房间不存在');

      touchRoom(roomCode);
      const triggeredBy = socket.data.playerId || 'unknown';
      io.to(roomCode).emit('jumpscare', { type, triggeredBy });
      console.log(`[jumpscare] 房间 ${roomCode} 触发 ${type}，由 ${triggeredBy} 触发`);
    } catch (err) {
      console.error('[triggerJumpscare] 错误:', err.message);
      socket.emit('error', { message: err.message });
    }
  });

  /**
   * stageChange { roomCode, stage }
   * 阶段变更广播。
   */
  socket.on('stageChange', (payload) => {
    try {
      const { roomCode, stage } = payload || {};
      if (!roomCode || stage === undefined) throw new Error('缺少 roomCode 或 stage');

      const room = rooms.get(roomCode);
      if (!room) throw new Error('房间不存在');

      touchRoom(roomCode);
      const changedBy = socket.data.playerId || 'unknown';
      io.to(roomCode).emit('stageChanged', { stage, changedBy });
      broadcastSystemMessage(io, roomCode, `游戏阶段变更：${typeof stage === 'string' ? stage : JSON.stringify(stage)}`);
    } catch (err) {
      console.error('[stageChange] 错误:', err.message);
      socket.emit('error', { message: err.message });
    }
  });

  /**
   * playerReady { roomCode, ready }
   * 玩家准备状态变更广播。
   */
  socket.on('playerReady', (payload) => {
    try {
      const { roomCode, ready } = payload || {};
      if (!roomCode || ready === undefined) throw new Error('缺少 roomCode 或 ready');

      const room = rooms.get(roomCode);
      if (!room) throw new Error('房间不存在');

      const playerId = socket.data.playerId;
      if (!playerId) throw new Error('未找到玩家身份');
      const player = room.players.get(playerId);
      if (!player) throw new Error('玩家不在该房间');

      player.ready = !!ready;
      touchRoom(roomCode);

      io.to(roomCode).emit('playerReadyUpdate', { playerId, ready: player.ready, players: serializePlayers(room) });
    } catch (err) {
      console.error('[playerReady] 错误:', err.message);
      socket.emit('error', { message: err.message });
    }
  });

  /**
   * clueFound { roomCode, clue }
   * 线索发现广播（透传）。
   */
  socket.on('clueFound', (payload) => {
    try {
      const { roomCode, clue } = payload || {};
      if (!roomCode) throw new Error('缺少 roomCode');
      const room = rooms.get(roomCode);
      if (!room) throw new Error('房间不存在');
      touchRoom(roomCode);
      io.to(roomCode).emit('clueFound', { clue, foundBy: socket.data.playerId || 'unknown' });
    } catch (err) {
      socket.emit('error', { message: err.message });
    }
  });

  /**
   * voteCast { roomCode, vote }
   * 投票广播（透传）。
   */
  socket.on('voteCast', (payload) => {
    try {
      const { roomCode, vote } = payload || {};
      if (!roomCode) throw new Error('缺少 roomCode');
      const room = rooms.get(roomCode);
      if (!room) throw new Error('房间不存在');
      touchRoom(roomCode);
      io.to(roomCode).emit('voteCast', { vote, votedBy: socket.data.playerId || 'unknown' });
    } catch (err) {
      socket.emit('error', { message: err.message });
    }
  });

  /**
   * 自定义事件透传：
   * 前端发送任意 { eventName, roomCode, data }，
   * 后端在房间内广播 <eventName> 事件，附带 data 和 senderId。
   *
   * 也支持前端直接 emit 任意事件并附带 roomCode，本通用透传兜底。
   */
  socket.on('customEvent', (payload) => {
    try {
      const { eventName, roomCode, data } = payload || {};
      if (!eventName || !roomCode) throw new Error('缺少 eventName 或 roomCode');
      const room = rooms.get(roomCode);
      if (!room) throw new Error('房间不存在');
      touchRoom(roomCode);
      io.to(roomCode).emit(eventName, { ...(data || {}), senderId: socket.data.playerId || 'unknown' });
    } catch (err) {
      socket.emit('error', { message: err.message });
    }
  });

  // -----------------------------------------------------------------------
  // 断线处理
  // -----------------------------------------------------------------------
  socket.on('disconnect', () => {
    const { roomCode, playerId } = socket.data;
    if (!roomCode || !playerId) {
      console.log(`[disconnect] socket ${socket.id} 未关联房间，直接断开`);
      return;
    }

    const room = rooms.get(roomCode);
    if (!room) return;

    const player = room.players.get(playerId);
    if (!player) return;

    player.online = false;
    player.socketId = null;
    touchRoom(roomCode);

    console.log(`[disconnect] ${player.nickname}(${playerId}) 从房间 ${roomCode} 断线`);

    // 广播离线状态
    io.to(roomCode).emit('playerStatusChange', { playerId, online: false });
    broadcastSystemMessage(io, roomCode, `${player.nickname} 离线（${RECONNECT_GRACE_MS / 60000} 分钟内可重连）`);

    // 如果房主离线，自动把房主转移给最早在线的玩家
    if (player.isHost) {
      const newHost = Array.from(room.players.values()).find(p => p.online && p.id !== playerId);
      if (newHost) {
        newHost.isHost = true;
        room.hostId = newHost.id;
        io.to(roomCode).emit('hostChanged', { hostId: newHost.id });
        broadcastSystemMessage(io, roomCode, `${newHost.nickname} 成为新房主`);
        console.log(`[hostChange] 房间 ${roomCode} 新房主: ${newHost.nickname}(${newHost.id})`);
      }
    }

    // 如果房间内没有任何在线玩家，标记但不立即删除（由 GC 清理）
    const onlineCount = Array.from(room.players.values()).filter(p => p.online).length;
    if (onlineCount === 0) {
      console.log(`[idle] 房间 ${roomCode} 当前无在线玩家，等待 GC 清理`);
    }
  });
});

// ---------------------------------------------------------------------------
// 启动
// ---------------------------------------------------------------------------
httpServer.listen(PORT, () => {
  console.log('==========================================');
  console.log('  剧本杀联机服务器');
  console.log('  WebSocket server running on port ' + PORT);
  console.log('  支持游戏: guianlu / fenxinlu / jinlou');
  console.log('  空闲房间超时: ' + ROOM_IDLE_TIMEOUT_MS / 60000 + ' 分钟');
  console.log('  每房间最大人数: ' + MAX_PLAYERS_PER_ROOM);
  console.log('==========================================');
});

module.exports = { app, httpServer, io };
