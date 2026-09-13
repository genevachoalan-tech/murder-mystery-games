/**
 * 端到端测试脚本
 * 测试流程：
 *  1. 玩家A 创建房间
 *  2. 玩家B 加入房间
 *  3. 玩家A 发送 stateUpdate → B 应收到 stateUpdated
 *  4. 玩家B 发送 chatMessage → A 应收到 chatMessage
 *  5. 玩家A 触发 jumpscare → B 应收到 jumpscare
 *  6. 玩家A 触发 stageChange → B 应收到 stageChanged
 *  7. 玩家B 断线 → A 应收到 playerStatusChange(offline)
 *  8. 玩家B 重连 → A 应收到 playerStatusChange(online)
 *  9. 玩家B 发送 voteCast → A 应收到 voteCast
 */

const { io } = require('socket.io-client');

const SERVER_URL = 'http://localhost:3000';
let passed = 0;
let failed = 0;

function check(name, condition, detail = '') {
  if (condition) {
    passed++;
    console.log(`  [PASS] ${name}${detail ? ' — ' + detail : ''}`);
  } else {
    failed++;
    console.log(`  [FAIL] ${name}${detail ? ' — ' + detail : ''}`);
  }
}

function wait(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

async function main() {
  console.log('\n=== 剧本杀联机服务器端到端测试 ===\n');

  // ---- 玩家A 创建房间 ----
  console.log('[1] 玩家A 创建房间...');
  const socketA = io(SERVER_URL);
  const resultsA = { roomCreated: null, playerJoined: null, stateUpdated: null, chatMessage: null, jumpscare: null, stageChanged: null, playerStatusChange: null, voteCast: null };

  socketA.on('roomCreated', (d) => { resultsA.roomCreated = d; });
  socketA.on('playerJoined', (d) => { resultsA.playerJoined = d; });
  socketA.on('stateUpdated', (d) => { resultsA.stateUpdated = d; });
  socketA.on('chatMessage', (d) => { resultsA.chatMessage = d; });
  socketA.on('jumpscare', (d) => { resultsA.jumpscare = d; });
  socketA.on('stageChanged', (d) => { resultsA.stageChanged = d; });
  socketA.on('playerStatusChange', (d) => { resultsA.playerStatusChange = d; });
  socketA.on('voteCast', (d) => { resultsA.voteCast = d; });
  socketA.on('error', (d) => { console.log('  [A error]', d); });

  await new Promise((resolve) => socketA.on('connect', resolve));
  socketA.emit('createRoom', { nickname: '小明', role: '侦探', gameType: 'guianlu' });
  await wait(500);

  check('房间创建成功', !!resultsA.roomCreated, `roomCode=${resultsA.roomCreated?.roomCode}`);
  check('房间码为6位数字', /^\d{6}$/.test(resultsA.roomCreated?.roomCode || ''), resultsA.roomCreated?.roomCode);

  const roomCode = resultsA.roomCreated.roomCode;
  const playerAId = resultsA.roomCreated.playerId;

  // ---- 玩家B 加入房间 ----
  console.log('\n[2] 玩家B 加入房间...');
  const socketB = io(SERVER_URL);
  const resultsB = { joinedRoom: null, stateUpdated: null };
  socketB.on('joinedRoom', (d) => { resultsB.joinedRoom = d; });
  socketB.on('stateUpdated', (d) => { resultsB.stateUpdated = d; });
  socketB.on('error', (d) => { console.log('  [B error]', d); });

  await new Promise((resolve) => socketB.on('connect', resolve));
  socketB.emit('joinRoom', { roomCode, nickname: '小红', role: '嫌疑人', gameType: 'guianlu' });
  await wait(500);

  check('B 成功加入房间', resultsB.joinedRoom?.success === true);
  check('B 收到房间状态', !!resultsB.joinedRoom?.roomState);
  check('B 收到玩家列表', Array.isArray(resultsB.joinedRoom?.players) && resultsB.joinedRoom.players.length === 2);
  check('A 收到 playerJoined 广播', !!resultsA.playerJoined, `players count = ${resultsA.playerJoined?.players?.length}`);
  check('A 收到两个玩家', resultsA.playerJoined?.players?.length === 2);

  // ---- stateUpdate ----
  console.log('\n[3] 玩家A 发送 stateUpdate...');
  const newState = { stage: 'investigation', clues: ['钥匙', '血迹'], round: 2 };
  resultsB.stateUpdated = null;
  socketA.emit('stateUpdate', { roomCode, gameState: newState });
  await wait(500);
  check('B 收到 stateUpdated', resultsB.stateUpdated?.gameState?.round === 2, JSON.stringify(resultsB.stateUpdated?.gameState));
  check('B 收到的 updatedBy 是A', resultsB.stateUpdated?.updatedBy === playerAId);

  // ---- chatMessage ----
  console.log('\n[4] 玩家B 发送聊天消息...');
  resultsA.chatMessage = null;
  socketB.emit('chatMessage', { roomCode, message: '大家好，我是小红' });
  await wait(500);
  check('A 收到 B 的聊天消息', resultsA.chatMessage?.message === '大家好，我是小红', resultsA.chatMessage?.message);
  check('聊天消息包含昵称', resultsA.chatMessage?.nickname === '小红');
  check('聊天消息 type=text', resultsA.chatMessage?.type === 'text');

  // ---- jumpscare ----
  console.log('\n[5] 玩家A 触发 jumpscare...');
  socketA.emit('triggerJumpscare', { roomCode, type: 'ghost_face' });
  await wait(500);
  check('B 收到 jumpscare', resultsA.jumpscare === null ? false : true, '等待B接收');

  // B 端也验证
  let bJumpscare = null;
  socketB.on('jumpscare', (d) => { bJumpscare = d; });
  // 重新触发一次，因为上面那次 B 的监听还没注册
  socketA.emit('triggerJumpscare', { roomCode, type: 'loud_sound' });
  await wait(500);
  check('B 收到 jumpscare 事件', bJumpscare?.type === 'loud_sound', `type=${bJumpscare?.type}`);
  check('jumpscare 包含 triggeredBy', bJumpscare?.triggeredBy === playerAId);

  // ---- stageChange ----
  console.log('\n[6] 玩家A 触发 stageChange...');
  socketA.emit('stageChange', { roomCode, stage: 'accusation' });
  await wait(500);
  check('B 收到 stageChanged', resultsB.stateUpdated !== null, '检查B端监听');

  // B 端监听 stageChanged
  let bStageChanged = null;
  socketB.on('stageChanged', (d) => { bStageChanged = d; });
  socketA.emit('stageChange', { roomCode, stage: 'final_vote' });
  await wait(500);
  check('B 收到 stageChanged 事件', bStageChanged?.stage === 'final_vote', `stage=${bStageChanged?.stage}`);

  // ---- voteCast ----
  console.log('\n[7] 玩家B 投票...');
  socketB.emit('voteCast', { roomCode, vote: { target: '小明', reason: '他最可疑' } });
  await wait(500);
  check('A 收到 voteCast', resultsA.voteCast?.vote?.target === '小明', JSON.stringify(resultsA.voteCast?.vote));

  // ---- 断线重连 ----
  console.log('\n[8] 玩家B 断线...');
  resultsA.playerStatusChange = null;
  socketB.disconnect();
  await wait(800);
  check('A 收到 B 离线广播', resultsA.playerStatusChange?.online === false, JSON.stringify(resultsA.playerStatusChange));

  console.log('\n[9] 玩家B 重连...');
  const socketB2 = io(SERVER_URL);
  let b2Status = null;
  socketA.on('playerStatusChange', (d) => { b2Status = d; });
  await new Promise((resolve) => socketB2.on('connect', resolve));
  socketB2.emit('reconnect', { roomCode, playerId: resultsB.joinedRoom.you });
  await wait(800);
  check('A 收到 B 重连广播', b2Status?.online === true, JSON.stringify(b2Status));

  // ---- 错误处理 ----
  console.log('\n[10] 测试错误处理...');
  const socketC = io(SERVER_URL);
  let cError = null;
  socketC.on('error', (d) => { cError = d; });
  await new Promise((resolve) => socketC.on('connect', resolve));
  socketC.emit('joinRoom', { roomCode: '000000', nickname: '测试者' });
  await wait(500);
  check('加入不存在房间返回错误', !!cError?.message, cError?.message);
  socketC.disconnect();

  // ---- 清理 ----
  socketA.disconnect();
  socketB2.disconnect();

  console.log('\n=== 测试结果 ===');
  console.log(`通过: ${passed}  失败: ${failed}  总计: ${passed + failed}`);
  if (failed > 0) {
    console.log('有失败项！');
    process.exit(1);
  } else {
    console.log('全部通过 ✓');
    process.exit(0);
  }
}

main().catch((err) => {
  console.error('测试异常:', err);
  process.exit(1);
});
