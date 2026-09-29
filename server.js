const express = require('express');
const http = require('http');
const crypto = require('crypto');
const { Server } = require('socket.io');

const app = express();
const server = http.createServer(app);
const io = new Server(server, { pingTimeout: 20000, pingInterval: 10000 });
app.use(express.static('public'));

const PORT = process.env.PORT || 3000;
const rooms = new Map();
const SESSION_TTL = 24 * 60 * 60 * 1000;

const PAIRS = [
  ['咖啡','咖啡','kāfēi','coffee','珍珠奶茶','珍珠奶茶','zhēnzhū nǎichá','bubble tea'],
  ['可樂','可乐','kělè','cola','雪碧','雪碧','xuěbì','Sprite'],
  ['漢堡','汉堡','hànbǎo','hamburger','披薩','披萨','pīsà','pizza'],
  ['炸雞','炸鸡','zhájī','fried chicken','薯條','薯条','shǔtiáo','French fries'],
  ['火鍋','火锅','huǒguō','hot pot','燒烤','烧烤','shāokǎo','barbecue'],
  ['壽司','寿司','shòusī','sushi','生魚片','生鱼片','shēngyúpiàn','sashimi'],
  ['拉麵','拉面','lāmiàn','ramen','牛肉麵','牛肉面','niúròu miàn','beef noodles'],
  ['蛋糕','蛋糕','dàngāo','cake','冰淇淋','冰淇淋','bīngqílín','ice cream'],
  ['蘋果','苹果','píngguǒ','apple','香蕉','香蕉','xiāngjiāo','banana'],
  ['西瓜','西瓜','xīguā','watermelon','哈密瓜','哈密瓜','hāmìguā','melon'],
  ['草莓','草莓','cǎoméi','strawberry','櫻桃','樱桃','yīngtáo','cherry'],
  ['芒果','芒果','mángguǒ','mango','鳳梨','凤梨','fènglí','pineapple'],
  ['貓','猫','māo','cat','狗','狗','gǒu','dog'],
  ['老虎','老虎','lǎohǔ','tiger','獅子','狮子','shīzi','lion'],
  ['熊貓','熊猫','xióngmāo','panda','無尾熊','无尾熊','wúwěixióng','koala'],
  ['企鵝','企鹅','qǐ é','penguin','海豹','海豹','hǎibào','seal'],
  ['籃球','篮球','lánqiú','basketball','足球','足球','zúqiú','football'],
  ['羽毛球','羽毛球','yǔmáoqiú','badminton','網球','网球','wǎngqiú','tennis'],
  ['棒球','棒球','bàngqiú','baseball','壘球','垒球','lěiqiú','softball'],
  ['游泳','游泳','yóuyǒng','swimming','潛水','潜水','qiánshuǐ','diving'],
  ['鋼琴','钢琴','gāngqín','piano','吉他','吉他','jítā','guitar'],
  ['小提琴','小提琴','xiǎotíqín','violin','大提琴','大提琴','dàtíqín','cello'],
  ['電影','电影','diànyǐng','movie','電視劇','电视剧','diànshìjù','TV drama'],
  ['演唱會','演唱会','yǎnchànghuì','concert','音樂祭','音乐祭','yīnyuèjì','music festival'],
  ['手機','手机','shǒujī','smartphone','平板','平板','píngbǎn','tablet'],
  ['筆記型電腦','笔记型电脑','bǐjìxíng diànnǎo','laptop','桌上型電腦','桌上型电脑','zhuōshàngxíng diànnǎo','desktop computer'],
  ['捷運','捷运','jiéyùn','metro','火車','火车','huǒchē','train'],
  ['公車','公车','gōngchē','bus','計程車','计程车','jìchéngchē','taxi'],
  ['汽車','汽车','qìchē','car','機車','机车','jīchē','motorcycle'],
  ['飛機','飞机','fēijī','airplane','直升機','直升机','zhíshēngjī','helicopter']
];

function word(a,b,c,d){ return { trad:a, simp:b, pinyin:c, en:d }; }
function pickPair(){
  const p=PAIRS[Math.floor(Math.random()*PAIRS.length)];
  return { civilian:word(p[0],p[1],p[2],p[3]), undercover:word(p[4],p[5],p[6],p[7]) };
}
function token(){ return crypto.randomBytes(18).toString('base64url'); }
function code(v){ return String(v||'').trim().toUpperCase(); }
function cleanName(v){ return String(v||'').trim().replace(/\s+/g,' ').slice(0,20); }
function roomOf(socket){ return socket.data.roomCode ? rooms.get(socket.data.roomCode) : null; }
function playerOf(socket){ const r=roomOf(socket); return r?.players.get(socket.data.playerId) || null; }
function getRoom(roomCode){
  const c=code(roomCode);
  if(!rooms.has(c)) rooms.set(c,{ code:c, phase:'lobby', players:new Map(), gameId:null, roundNo:0, pair:null, undercoverId:null, aliveIds:new Set(), votes:new Map(), result:null, nextReady:new Set(), newReady:new Set(), createdAt:Date.now() });
  return rooms.get(c);
}
function activePlayers(r){ return [...r.players.values()].filter(p=>p.connected); }
function inGamePlayers(r){ return [...r.players.values()].filter(p=>p.inGame); }
function counts(r){ let spy=0,civ=0; for(const id of r.aliveIds){ id===r.undercoverId?spy++:civ++; } return {spy,civ}; }
function publicPlayer(r,p,viewerId){ return { id:p.id,name:p.name,ready:p.ready,connected:p.connected,inGame:!!p.inGame,isUndercover:(r.phase==='final' ? p.id===r.undercoverId : undefined) }; }
function snapshot(r,viewerId){
  const me=r.players.get(viewerId);
  let phase=r.phase;
  if(r.gameId && (!me?.inGame || !r.aliveIds.has(viewerId)) && !['final','roundResult'].includes(phase)) phase='spectator';
  const myWord = r.gameId && me?.inGame ? (viewerId===r.undercoverId?r.pair?.undercover:r.pair?.civilian) : null;
  let result=r.result ? {...r.result} : null;
  if(result && r.phase==='final') result={...result,pair:r.pair,undercoverName:r.players.get(r.undercoverId)?.name||''};
  return { roomCode:r.code,selfId:viewerId,phase,players:[...r.players.values()].map(p=>publicPlayer(r,p,viewerId)),aliveIds:[...r.aliveIds],myWord,gameId:r.gameId,roundNo:r.roundNo,result,votesCast:[...r.votes.keys()],nextReady:[...r.nextReady],newReady:[...r.newReady] };
}
function sync(r){ for(const p of r.players.values()) if(p.connected && p.socketId) io.to(p.socketId).emit('snapshot',snapshot(r,p.id)); }
function notice(r,zh,en,type='warn'){ io.to(r.code).emit('notice',{zh,en,type}); }
function resetReady(r){ for(const p of r.players.values()) p.ready=false; }
function startGame(r){
  const ps=activePlayers(r);
  if(ps.length<3 || ps.some(p=>!p.ready) || r.gameId) return;
  r.gameId=Date.now().toString(36)+'-'+crypto.randomBytes(3).toString('hex'); r.roundNo=1; r.phase='game'; r.pair=pickPair(); r.result=null; r.votes.clear(); r.nextReady.clear(); r.newReady.clear();
  for(const p of r.players.values()) p.inGame=false;
  for(const p of ps) p.inGame=true;
  r.aliveIds=new Set(ps.map(p=>p.id)); r.undercoverId=ps[Math.floor(Math.random()*ps.length)].id; resetReady(r); sync(r);
}
function checkStart(r){ if(r.phase==='lobby' && !r.gameId) startGame(r); }
function resolveVote(r){
  if(r.phase!=='vote') return;
  const alive=[...r.aliveIds]; if(alive.length<2 || alive.some(id=>!r.votes.has(id))) return;
  const tally=new Map(); for(const [voter,target] of r.votes){ if(r.aliveIds.has(voter)&&r.aliveIds.has(target)) tally.set(target,(tally.get(target)||0)+1); }
  const max=Math.max(0,...tally.values()); const top=[...tally.entries()].filter(([,n])=>n===max).map(([id])=>id);
  const res={gameId:r.gameId,roundNo:r.roundNo,aliveIds:[...r.aliveIds],winner:null,tie:false,eliminatedId:null,eliminatedWasSpy:false,civiliansAlive:0,spiesAlive:0};
  if(top.length!==1){ res.tie=true; }
  else { const out=top[0]; res.eliminatedId=out; res.eliminatedWasSpy=out===r.undercoverId; r.aliveIds.delete(out); res.aliveIds=[...r.aliveIds]; }
  const c=counts(r); res.civiliansAlive=c.civ; res.spiesAlive=c.spy; if(c.spy===0) res.winner='civilian'; else if(c.civ<=c.spy) res.winner='undercover';
  r.result=res; r.phase=res.winner?'final':'roundResult'; r.nextReady.clear(); r.newReady.clear(); sync(r);
}
function maybeNextRound(r){ if(r.phase!=='roundResult'||r.result?.winner) return; const alive=[...r.aliveIds]; if(alive.length && alive.every(id=>r.nextReady.has(id))){ r.roundNo++; r.phase='game'; r.votes.clear(); r.result=null; r.nextReady.clear(); sync(r); } }
function maybeNewGame(r){
  if(r.phase!=='final') return; const ps=activePlayers(r); if(ps.length>=3 && ps.every(p=>r.newReady.has(p.id))){
    r.gameId=null; r.phase='lobby'; r.roundNo=0; r.pair=null; r.undercoverId=null; r.aliveIds.clear(); r.votes.clear(); r.result=null; r.nextReady.clear(); r.newReady.clear(); for(const p of r.players.values()){p.inGame=false;p.ready=true;} startGame(r);
  }
}
function checkQuitWin(r){
  if(!r.gameId || !['game','vote','roundResult'].includes(r.phase)) return;
  const c=counts(r); let winner=null; if(c.spy===0) winner='civilian'; else if(c.civ<=c.spy) winner='undercover';
  if(winner){ r.result={gameId:r.gameId,roundNo:r.roundNo,aliveIds:[...r.aliveIds],winner,tie:false,eliminatedId:null,eliminatedWasSpy:false,civiliansAlive:c.civ,spiesAlive:c.spy}; r.phase='final'; r.votes.clear(); r.nextReady.clear(); r.newReady.clear(); }
}

io.on('connection', socket => {
  socket.on('joinRoom',(data={},ack=()=>{})=>{
    const roomCode=code(data.roomCode), name=cleanName(data.name), incomingToken=String(data.token||'');
    if(!/^[A-Z2-9]{6}$/.test(roomCode)) return ack({ok:false,error:'Invalid room code',errorZh:'房間碼格式錯誤。'});
    if(!name) return ack({ok:false,error:'Name is required',errorZh:'請輸入名字。'});
    const r=getRoom(roomCode);
    let p=null;
    if(incomingToken) p=[...r.players.values()].find(x=>x.token===incomingToken) || null;
    if(!p) p=[...r.players.values()].find(x=>x.name.toLocaleLowerCase()===name.toLocaleLowerCase() && !x.connected) || null;
    if(!p && [...r.players.values()].some(x=>x.name.toLocaleLowerCase()===name.toLocaleLowerCase() && x.connected)) return ack({ok:false,error:'That name is already in use',errorZh:'這個名字目前已有人使用。'});
    const isNewPlayer=!p;
    if(!p){ p={id:crypto.randomUUID(),token:token(),name,ready:false,connected:true,socketId:socket.id,lastSeen:Date.now(),inGame:false}; r.players.set(p.id,p); }
    p.name=name; p.connected=true; p.socketId=socket.id; p.lastSeen=Date.now();

    // 每輪結束（roundResult）到下一輪開始前，允許新玩家直接加入下一輪。
    // 為了不在同一局中途改變臥底身份，新加入者固定以平民身份加入，並使用同一組平民詞。
    if(isNewPlayer && r.gameId && r.phase==='roundResult' && !r.result?.winner){
      p.inGame=true;
      p.ready=false;
      r.aliveIds.add(p.id);
      r.nextReady.delete(p.id);
      if(r.result){
        const c=counts(r);
        r.result.aliveIds=[...r.aliveIds];
        r.result.civiliansAlive=c.civ;
        r.result.spiesAlive=c.spy;
      }
    }

    socket.data.roomCode=roomCode; socket.data.playerId=p.id; socket.join(roomCode);
    ack({ok:true,roomCode,token:p.token,snapshot:snapshot(r,p.id)});
    sync(r);
    if(isNewPlayer && r.gameId && r.phase==='roundResult' && !r.result?.winner){
      notice(r,`${p.name} 已加入，會從下一輪以平民身份參賽。`,`${p.name} joined and will enter the next round as a civilian.`,'ok');
    }
  });
  socket.on('setReady',({ready}={})=>{const r=roomOf(socket),p=playerOf(socket);if(!r||!p||r.phase!=='lobby')return;p.ready=!!ready;sync(r);checkStart(r);});
  socket.on('openVote',()=>{const r=roomOf(socket),p=playerOf(socket);if(!r||!p||r.phase!=='game'||!r.aliveIds.has(p.id))return;r.phase='vote';r.votes.clear();sync(r);});
  socket.on('vote',({targetId}={})=>{const r=roomOf(socket),p=playerOf(socket);if(!r||!p||r.phase!=='vote'||!r.aliveIds.has(p.id)||r.votes.has(p.id)||!r.aliveIds.has(targetId)||targetId===p.id)return;r.votes.set(p.id,targetId);sync(r);resolveVote(r);});
  socket.on('nextReady',({ready}={})=>{const r=roomOf(socket),p=playerOf(socket);if(!r||!p||r.phase!=='roundResult'||!r.aliveIds.has(p.id))return;ready?r.nextReady.add(p.id):r.nextReady.delete(p.id);sync(r);maybeNextRound(r);});
  socket.on('newReady',({ready}={})=>{const r=roomOf(socket),p=playerOf(socket);if(!r||!p||r.phase!=='final')return;ready?r.newReady.add(p.id):r.newReady.delete(p.id);sync(r);maybeNewGame(r);});
  socket.on('requestSnapshot',()=>{const r=roomOf(socket),p=playerOf(socket);if(r&&p)socket.emit('snapshot',snapshot(r,p.id));});
  socket.on('leaveRoom',(_,ack=()=>{})=>{const r=roomOf(socket),p=playerOf(socket);if(!r||!p)return ack({ok:true});if(r.aliveIds.has(p.id))r.aliveIds.delete(p.id);r.players.delete(p.id);r.votes.delete(p.id);r.nextReady.delete(p.id);r.newReady.delete(p.id);socket.leave(r.code);socket.data.roomCode=null;socket.data.playerId=null;checkQuitWin(r);sync(r);ack({ok:true});});
  socket.on('disconnect',()=>{const r=roomOf(socket),p=playerOf(socket);if(!r||!p)return;p.connected=false;p.socketId=null;p.lastSeen=Date.now();sync(r);notice(r,`${p.name} 暫時離線；伺服器不會自動淘汰或代替投票。`,`${p.name} is temporarily offline. The server will not auto-eliminate them or cast a vote for them.`,'warn');});
});

setInterval(()=>{
  const now=Date.now();
  for(const [roomCode,r] of rooms){
    for(const [id,p] of r.players){ if(!p.connected && now-p.lastSeen>SESSION_TTL && !r.aliveIds.has(id)) r.players.delete(id); }
    if(r.players.size===0 && now-r.createdAt>SESSION_TTL) rooms.delete(roomCode);
  }
},60*60*1000).unref();

server.listen(PORT,()=>console.log(`Undercover server running on http://localhost:${PORT}`));
