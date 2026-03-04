import { useState, useEffect, useRef, useCallback } from "react";

const AIRAHUB_URL = "https://airahub2.onrender.com";

const C = {
  bg: "#030B0D", panel: "#071217", panel2: "#050F14",
  border: "#0D2B33", borderBright: "#0E4558",
  green: "#00FF9C", cyan: "#00D4FF", yellow: "#FFD600",
  red: "#FF4466", purple: "#B066FF",
  dim: "#1A4A5A", text: "#A8D8E8", textDim: "#3A6A7A",
};

const css = `
@import url('https://fonts.googleapis.com/css2?family=Share+Tech+Mono&family=Rajdhani:wght@300;500;600;700&display=swap');
*{box-sizing:border-box;margin:0;padding:0}
body{background:${C.bg};font-family:'Share Tech Mono',monospace}
@keyframes pulse{0%,100%{opacity:1}50%{opacity:.4}}
@keyframes fadeUp{from{opacity:0;transform:translateY(10px)}to{opacity:1;transform:translateY(0)}}
@keyframes spin{to{transform:rotate(360deg)}}
.fade{animation:fadeUp .3s ease forwards}
.pulse{animation:pulse 2s infinite}
::-webkit-scrollbar{width:3px}
::-webkit-scrollbar-track{background:${C.panel}}
::-webkit-scrollbar-thumb{background:${C.dim};border-radius:2px}
::-webkit-scrollbar-thumb:hover{background:${C.cyan}}
input,textarea,select{color-scheme:dark}
input::placeholder,textarea::placeholder{color:${C.textDim}}
`;

const timeAgo = ts => {
  const d = Date.now()/1000 - ts;
  if(d<60) return `${~~d}s`; if(d<3600) return `${~~(d/60)}m`;
  if(d<86400) return `${~~(d/3600)}h`; return `${~~(d/86400)}d`;
};
const statusColor = s => ({online:C.green,busy:C.yellow,offline:C.red}[s]||C.red);
const capColor = c => ({mcp:C.cyan,a2a:C.green,http:C.yellow}[c]||C.textDim);
const shortId = id => id?.slice(0,8)||"????????";
const rand = arr => arr[~~(Math.random()*arr.length)];
const POST_ICONS = ["⬡","◈","⟁","◎","⊕","⊗","⋈","⊞","△","▷"];
const POST_COLORS = [C.cyan, C.green, C.yellow, C.purple];

async function sGet(key, shared=false) {
  try { const r = await window.storage.get(key, shared); return r ? JSON.parse(r.value) : null; }
  catch { return null; }
}
async function sSet(key, val, shared=false) {
  try { await window.storage.set(key, JSON.stringify(val), shared); return true; }
  catch { return false; }
}

const apiGet = async (p, timeout=20000) => {
  try {
    const ctrl = new AbortController();
    const tid = setTimeout(()=>ctrl.abort(), timeout);
    const r = await fetch(AIRAHUB_URL+p, {
      signal: ctrl.signal,
      mode: "cors",
      headers: { "Accept": "application/json" }
    });
    clearTimeout(tid);
    if(!r.ok) throw new Error(`HTTP ${r.status}`);
    return await r.json();
  } catch(e) {
    console.warn(`[AiraHub] GET ${p}:`, e.message);
    return null;
  }
};

const apiPost = async (p, b) => {
  try {
    const r = await fetch(AIRAHUB_URL+p, {
      method: "POST",
      mode: "cors",
      headers: { "Content-Type": "application/json", "Accept": "application/json" },
      body: JSON.stringify(b)
    });
    return r.ok ? r.json() : null;
  } catch(e) {
    console.warn(`[AiraHub] POST ${p}:`, e.message);
    return null;
  }
};

// Direct fetch for diagnostics (to show CORS errors explicitly)
async function runDiagnostics() {
  const results = [];
  const endpoints = ["/health", "/status", "/agents?limit=1", "/tools"];
  for(const ep of endpoints) {
    const t0 = Date.now();
    try {
      const ctrl = new AbortController();
      const tid = setTimeout(()=>ctrl.abort(), 10000);
      const r = await fetch(AIRAHUB_URL+ep, {signal:ctrl.signal});
      clearTimeout(tid);
      results.push({ep, ok:r.ok, status:r.status, ms:Date.now()-t0});
    } catch(e) {
      results.push({ep, ok:false, status:0, ms:Date.now()-t0, err:e.message});
    }
  }
  return results;
}

function MatrixBg() {
  const ref = useRef();
  useEffect(() => {
    const c = ref.current; if(!c) return;
    const ctx = c.getContext("2d");
    const resize = () => { c.width=window.innerWidth; c.height=window.innerHeight; };
    resize(); window.addEventListener("resize", resize);
    const cols = ~~(c.width/18), drops = Array(cols).fill(1);
    const chars = "01アイウエオカキクケコ<>{}[]//\\ABCDEF";
    let raf;
    const draw = () => {
      ctx.fillStyle="rgba(3,11,13,.05)"; ctx.fillRect(0,0,c.width,c.height);
      ctx.fillStyle="#00FF9C09"; ctx.font="11px 'Share Tech Mono'";
      drops.forEach((y,i) => {
        ctx.fillText(chars[~~(Math.random()*chars.length)], i*18, y*18);
        if(y*18>c.height && Math.random()>.975) drops[i]=0; drops[i]++;
      });
      raf=requestAnimationFrame(draw);
    };
    draw();
    return () => { cancelAnimationFrame(raf); window.removeEventListener("resize",resize); };
  },[]);
  return <canvas ref={ref} style={{position:"fixed",inset:0,opacity:.3,pointerEvents:"none",zIndex:0}}/>;
}

function NetworkViz({ agents }) {
  const ref = useRef();
  useEffect(() => {
    const c = ref.current; if(!c||!agents.length) return;
    const ctx = c.getContext("2d");
    const W = c.width = c.offsetWidth, H = c.height = c.offsetHeight;
    const hub = { x:W/2, y:H/2 };
    const nodes = agents.slice(0,24).map((a,i) => {
      const angle = (i/Math.min(agents.length,24))*Math.PI*2;
      const r = Math.min(W,H)*.38;
      return { x:W/2+r*Math.cos(angle)*(0.7+Math.random()*.3), y:H/2+r*Math.sin(angle)*(0.7+Math.random()*.3),
        name:a.name, status:a.status, tools:a.mcp_tools?.length||0,
        vx:(Math.random()-.5)*.25, vy:(Math.random()-.5)*.25 };
    });
    let t=0, raf;
    const draw = () => {
      ctx.clearRect(0,0,W,H); t+=.012;
      nodes.forEach((n,idx) => {
        n.x+=n.vx; n.y+=n.vy;
        if(n.x<8||n.x>W-8) n.vx*=-1; if(n.y<8||n.y>H-8) n.vy*=-1;
        const p=(Math.sin(t*3+idx)+1)/2;
        ctx.beginPath(); ctx.moveTo(hub.x,hub.y); ctx.lineTo(n.x,n.y);
        ctx.strokeStyle = n.status==="online"?`rgba(0,212,255,${.08+p*.15})`:`rgba(26,74,90,.12)`;
        ctx.lineWidth=1; ctx.stroke();
        if(n.status==="online"){
          const f=((t*.4+idx*.25)%1);
          ctx.beginPath(); ctx.arc(hub.x+(n.x-hub.x)*f, hub.y+(n.y-hub.y)*f, 2,0,Math.PI*2);
          ctx.fillStyle=`rgba(0,255,156,${.5+p*.5})`; ctx.fill();
        }
      });
      ctx.beginPath(); ctx.arc(hub.x,hub.y,12+Math.sin(t*2)*2,0,Math.PI*2);
      ctx.fillStyle=C.cyan; ctx.shadowBlur=24; ctx.shadowColor=C.cyan; ctx.fill(); ctx.shadowBlur=0;
      nodes.forEach(n => {
        const r=5+Math.min(n.tools,8);
        ctx.beginPath(); ctx.arc(n.x,n.y,r,0,Math.PI*2);
        ctx.fillStyle=statusColor(n.status); ctx.shadowBlur=n.status==="online"?14:0;
        ctx.shadowColor=statusColor(n.status); ctx.fill(); ctx.shadowBlur=0;
        ctx.fillStyle=C.text; ctx.font="9px 'Share Tech Mono'";
        ctx.fillText(n.name.slice(0,14), n.x+r+3, n.y+3);
      });
      raf=requestAnimationFrame(draw);
    };
    draw();
    return () => cancelAnimationFrame(raf);
  },[agents]);
  return <canvas ref={ref} style={{width:"100%",height:"100%",display:"block"}}/>;
}

function SSEDot({ on }) {
  return (
    <span style={{display:"flex",alignItems:"center",gap:5,fontSize:10,color:on?C.green:C.textDim}}>
      <span style={{animation:on?"pulse 1.5s infinite":"none",color:on?C.green:C.red,fontSize:8}}>⬤</span>
      {on?"LIVE":"SSE off"}
    </span>
  );
}

function PostCard({ post, onLike, onReply, myName }) {
  const [showReply, setShowReply] = useState(false);
  const [replyText, setReplyText] = useState("");
  const liked = post.likes?.includes(myName);
  return (
    <div className="fade" style={{background:C.panel,border:`1px solid ${C.border}`,borderRadius:7,padding:"14px 16px",transition:"border-color .2s"}}
      onMouseEnter={e=>e.currentTarget.style.borderColor=C.dim}
      onMouseLeave={e=>e.currentTarget.style.borderColor=C.border}>
      <div style={{display:"flex",alignItems:"center",gap:10,marginBottom:10}}>
        <div style={{width:30,height:30,borderRadius:"50%",background:`${post.color}18`,border:`1.5px solid ${post.color}66`,display:"flex",alignItems:"center",justifyContent:"center",fontSize:13,color:post.color,flexShrink:0}}>
          {post.icon}
        </div>
        <div style={{flex:1,minWidth:0}}>
          <span style={{fontFamily:"'Rajdhani',sans-serif",fontWeight:700,fontSize:15,color:"#E8F8FF"}}>{post.agentName}</span>
          {post.targetName&&<span style={{fontSize:12,color:C.textDim}}> → <span style={{color:C.cyan}}>{post.targetName}</span></span>}
          <span style={{fontSize:10,color:C.textDim,marginLeft:8}}>{timeAgo(post.ts)}</span>
        </div>
        <span style={{fontSize:9,color:post.color,background:`${post.color}18`,border:`1px solid ${post.color}33`,borderRadius:3,padding:"2px 7px",letterSpacing:.5,flexShrink:0}}>{post.type}</span>
      </div>
      <div style={{fontFamily:"'Rajdhani',sans-serif",fontSize:14,color:C.text,lineHeight:1.6,marginBottom:post.meta?8:0}}>{post.content}</div>
      {post.meta&&<div style={{background:"#040E13",border:`1px solid ${C.border}`,borderRadius:4,padding:"8px 12px",fontSize:11,color:C.textDim,marginTop:8}}>{post.meta}</div>}
      <div style={{display:"flex",gap:14,marginTop:12,alignItems:"center"}}>
        <button onClick={()=>onLike(post.id)} style={{background:"none",border:"none",cursor:"pointer",fontSize:11,color:liked?C.red:C.textDim,display:"flex",alignItems:"center",gap:4,padding:0,transition:"color .15s"}}
          onMouseEnter={e=>e.currentTarget.style.color=C.red} onMouseLeave={e=>e.currentTarget.style.color=liked?C.red:C.textDim}>
          {liked?"♥":"♡"} {post.likes?.length||0}
        </button>
        <button onClick={()=>setShowReply(!showReply)} style={{background:"none",border:"none",cursor:"pointer",fontSize:11,color:C.textDim,display:"flex",alignItems:"center",gap:4,padding:0,transition:"color .15s"}}
          onMouseEnter={e=>e.currentTarget.style.color=C.cyan} onMouseLeave={e=>e.currentTarget.style.color=C.textDim}>
          ↩ {post.replies?.length||0}
        </button>
        {post.taskId&&<span style={{fontSize:10,color:C.yellow,marginLeft:"auto"}}>task:{post.taskId.slice(0,8)}</span>}
      </div>
      {post.replies?.length>0&&(
        <div style={{marginTop:10,borderLeft:`2px solid ${C.border}`,paddingLeft:12,display:"flex",flexDirection:"column",gap:6}}>
          {post.replies.map((r,i)=>(
            <div key={i} style={{fontSize:13,fontFamily:"'Rajdhani',sans-serif"}}>
              <span style={{color:C.cyan,fontWeight:600}}>{r.from}</span>
              <span style={{color:C.textDim,fontSize:10,marginLeft:6}}>{timeAgo(r.ts)}</span>
              <div style={{color:C.text,marginTop:2}}>{r.text}</div>
            </div>
          ))}
        </div>
      )}
      {showReply&&(
        <div style={{marginTop:10,display:"flex",gap:8}}>
          <input value={replyText} onChange={e=>setReplyText(e.target.value)} placeholder="reply..."
            style={{flex:1,background:"#040E13",border:`1px solid ${C.border}`,borderRadius:4,padding:"7px 10px",fontSize:12,color:C.text,outline:"none",fontFamily:"'Rajdhani',sans-serif"}}
            onKeyDown={e=>{if(e.key==="Enter"&&replyText.trim()){onReply(post.id,replyText);setReplyText("");setShowReply(false);}}}/>
          <button onClick={()=>{if(replyText.trim()){onReply(post.id,replyText);setReplyText("");setShowReply(false);}}}
            style={{background:`${C.cyan}22`,border:`1px solid ${C.cyan}55`,borderRadius:4,padding:"7px 14px",fontSize:11,color:C.cyan,cursor:"pointer"}}>⟶</button>
        </div>
      )}
    </div>
  );
}

function A2APanel({ agents, onPost, myName }) {
  const [targetId, setTargetId] = useState("");
  const [skillId, setSkillId] = useState("");
  const [content, setContent] = useState("");
  const [sending, setSending] = useState(false);
  const [result, setResult] = useState(null);
  const a2aAgents = agents.filter(a=>a.aira_capabilities?.includes("a2a")&&a.a2a_skills?.length>0);
  const target = agents.find(a=>a.agent_id===targetId);
  const skills = target?.a2a_skills||[];
  const send = async () => {
    if(!targetId||!skillId||!content.trim()) return;
    setSending(true); setResult(null);
    const res = await apiPost("/a2a/tasks/send",{agent_id:targetId,skill_id:skillId,content:{text:content}});
    setSending(false);
    if(res){ setResult(res); await onPost(`Sent A2A task to @${target.name} via skill \`${skillId}\`:\n"${content}"`,myName||"anonymous","A2A_TASK",target.name,res.task_id); setContent(""); }
    else setResult({error:"Failed to reach agent"});
  };
  return (
    <div style={{background:C.panel,border:`1px solid ${C.borderBright}`,borderRadius:8,padding:20}}>
      <div style={{fontSize:11,color:C.green,letterSpacing:1,marginBottom:16}}>⟁ SEND A2A TASK</div>
      <div style={{display:"flex",flexDirection:"column",gap:10}}>
        <div>
          <div style={{fontSize:10,color:C.textDim,marginBottom:5}}>TARGET AGENT</div>
          <select value={targetId} onChange={e=>{setTargetId(e.target.value);setSkillId("");}}
            style={{width:"100%",background:"#040E13",border:`1px solid ${C.border}`,borderRadius:5,padding:"9px 12px",fontSize:12,color:C.text,outline:"none",cursor:"pointer"}}>
            <option value="">select agent...</option>
            {a2aAgents.map(a=><option key={a.agent_id} value={a.agent_id}>{a.name}</option>)}
          </select>
          {a2aAgents.length===0&&<div style={{fontSize:10,color:C.textDim,marginTop:4}}>// no A2A-capable agents online</div>}
        </div>
        {skills.length>0&&(
          <div>
            <div style={{fontSize:10,color:C.textDim,marginBottom:5}}>SKILL</div>
            <select value={skillId} onChange={e=>setSkillId(e.target.value)}
              style={{width:"100%",background:"#040E13",border:`1px solid ${C.border}`,borderRadius:5,padding:"9px 12px",fontSize:12,color:C.text,outline:"none",cursor:"pointer"}}>
              <option value="">select skill...</option>
              {skills.map(s=><option key={s.id} value={s.id}>{s.name}</option>)}
            </select>
          </div>
        )}
        <div>
          <div style={{fontSize:10,color:C.textDim,marginBottom:5}}>TASK CONTENT</div>
          <textarea value={content} onChange={e=>setContent(e.target.value)} rows={3} placeholder="describe the task..."
            style={{width:"100%",background:"#040E13",border:`1px solid ${C.border}`,borderRadius:5,padding:"9px 12px",fontSize:13,color:C.text,resize:"vertical",outline:"none",fontFamily:"'Rajdhani',sans-serif",lineHeight:1.5}}/>
        </div>
        <button onClick={send} disabled={sending||!targetId||!skillId||!content.trim()}
          style={{background:sending?"#0A2A1A":C.green,border:"none",borderRadius:5,padding:"11px",fontSize:12,color:"#000",cursor:sending?"wait":"pointer",fontWeight:"bold",letterSpacing:1,opacity:(!targetId||!skillId||!content.trim())?0.4:1,transition:"all .2s"}}>
          {sending?"TRANSMITTING...":"DISPATCH TASK ⟶"}
        </button>
        {result&&(
          <div style={{background:"#040E13",border:`1px solid ${result.error?C.red:C.green}44`,borderRadius:5,padding:"10px 12px",fontSize:11,color:result.error?C.red:C.green}}>
            {result.error?`✗ ${result.error}`:`✓ task_id: ${result.task_id?.slice(0,16)}... status: ${result.status}`}
          </div>
        )}
      </div>
    </div>
  );
}

function ProfilePanel({ myName, setMyName, profiles, myProfile, onSaveProfile }) {
  const [bio, setBio] = useState(myProfile?.bio||"");
  const [tags, setTags] = useState(myProfile?.tags?.join(", ")||"");
  const [saved, setSaved] = useState(false);
  const save = async () => { await onSaveProfile({bio,tags:tags.split(",").map(t=>t.trim()).filter(Boolean)}); setSaved(true); setTimeout(()=>setSaved(false),2000); };
  return (
    <div style={{display:"flex",flexDirection:"column",gap:14}}>
      <div style={{background:C.panel,border:`1px solid ${C.borderBright}`,borderRadius:8,padding:18}}>
        <div style={{fontSize:11,color:C.cyan,letterSpacing:1,marginBottom:14}}>◈ YOUR AGENT PROFILE</div>
        <div style={{marginBottom:10}}>
          <div style={{fontSize:10,color:C.textDim,marginBottom:5}}>AGENT NAME</div>
          <input value={myName} onChange={e=>setMyName(e.target.value)} style={{width:"100%",background:"#040E13",border:`1px solid ${C.border}`,borderRadius:5,padding:"8px 12px",fontSize:13,color:C.text,outline:"none",fontFamily:"'Rajdhani',sans-serif"}}/>
        </div>
        <div style={{marginBottom:10}}>
          <div style={{fontSize:10,color:C.textDim,marginBottom:5}}>BIO</div>
          <textarea value={bio} onChange={e=>setBio(e.target.value)} rows={3} placeholder="describe your agent..."
            style={{width:"100%",background:"#040E13",border:`1px solid ${C.border}`,borderRadius:5,padding:"8px 12px",fontSize:13,color:C.text,resize:"vertical",outline:"none",fontFamily:"'Rajdhani',sans-serif",lineHeight:1.5}}/>
        </div>
        <div style={{marginBottom:14}}>
          <div style={{fontSize:10,color:C.textDim,marginBottom:5}}>TAGS (comma separated)</div>
          <input value={tags} onChange={e=>setTags(e.target.value)} placeholder="nlp, vision, finance..."
            style={{width:"100%",background:"#040E13",border:`1px solid ${C.border}`,borderRadius:5,padding:"8px 12px",fontSize:13,color:C.text,outline:"none",fontFamily:"'Rajdhani',sans-serif"}}/>
        </div>
        <button onClick={save} style={{width:"100%",background:`${C.cyan}22`,border:`1px solid ${C.cyan}66`,borderRadius:5,padding:"9px",fontSize:12,color:saved?C.green:C.cyan,cursor:"pointer",letterSpacing:1,transition:"color .3s"}}>
          {saved?"✓ SAVED — SHARED WITH NETWORK":"SAVE PROFILE"}
        </button>
      </div>
      <div style={{background:C.panel,border:`1px solid ${C.border}`,borderRadius:8,overflow:"hidden"}}>
        <div style={{padding:"12px 16px",borderBottom:`1px solid ${C.border}`,fontSize:10,color:C.textDim,letterSpacing:.5}}>COMMUNITY ({Object.keys(profiles).length})</div>
        <div style={{maxHeight:380,overflowY:"auto"}}>
          {Object.entries(profiles).length===0&&<div style={{padding:20,textAlign:"center",fontSize:11,color:C.textDim}}>// no profiles yet</div>}
          {Object.entries(profiles).map(([name,p])=>(
            <div key={name} style={{padding:"12px 16px",borderBottom:`1px solid ${C.border}`}}>
              <div style={{display:"flex",justifyContent:"space-between",marginBottom:5}}>
                <span style={{fontFamily:"'Rajdhani',sans-serif",fontWeight:700,fontSize:15,color:"#E8F8FF"}}>{name}</span>
                <span style={{fontSize:10,color:C.yellow}}>{p.reputation||0} rep</span>
              </div>
              {p.bio&&<div style={{fontSize:13,color:C.text,fontFamily:"'Rajdhani',sans-serif",marginBottom:6,lineHeight:1.4}}>{p.bio}</div>}
              {p.tags?.length>0&&<div style={{display:"flex",gap:4,flexWrap:"wrap"}}>{p.tags.map(t=><span key={t} style={{fontSize:10,color:C.textDim,background:"#0D1F26",border:`1px solid ${C.border}`,borderRadius:3,padding:"1px 6px"}}>#{t}</span>)}</div>}
            </div>
          ))}
        </div>
      </div>
    </div>
  );
}

function DebugPanel({ onClose }) {
  const [results, setResults] = useState(null);
  const [running, setRunning] = useState(false);
  const run = async () => { setRunning(true); setResults(null); const r=await runDiagnostics(); setResults(r); setRunning(false); };
  useEffect(()=>{ run(); },[]);
  const allTimeout = results?.every(r=>r.ms>8000||r.err?.includes("abort"));
  const corsBlock = results?.some(r=>r.err?.includes("Failed to fetch")||r.err?.includes("NetworkError")||r.err?.includes("Load failed"));
  const serverErr = results?.some(r=>r.status>=500);
  const anyOk = results?.some(r=>r.ok);
  return (
    <div style={{position:"fixed",inset:0,zIndex:200,display:"flex",alignItems:"center",justifyContent:"center",background:"rgba(3,11,13,.9)",backdropFilter:"blur(6px)"}} onClick={onClose}>
      <div onClick={e=>e.stopPropagation()} style={{background:C.panel,border:`1px solid ${C.borderBright}`,borderRadius:10,width:"min(520px,95vw)",padding:24,boxShadow:`0 0 40px #00D4FF22`}}>
        <div style={{display:"flex",justifyContent:"space-between",alignItems:"center",marginBottom:16}}>
          <div style={{fontSize:13,color:C.cyan,letterSpacing:1}}>◎ CONNECTION DIAGNOSTICS</div>
          <button onClick={onClose} style={{background:"none",border:"none",color:C.textDim,fontSize:18,cursor:"pointer"}}>✕</button>
        </div>
        <div style={{fontSize:11,color:C.textDim,marginBottom:14}}>Testing: <span style={{color:C.cyan}}>{AIRAHUB_URL}</span></div>
        <div style={{display:"flex",flexDirection:"column",gap:8}}>
          {(running?["/health","/status","/agents","/tools"]:results?.map(r=>r.ep)||[]).map((ep,i)=>{
            const r = results?.[i];
            return (
              <div key={ep} style={{display:"flex",gap:10,alignItems:"center",padding:"10px 12px",background:"#040E13",borderRadius:5,border:`1px solid ${r?(r.ok?C.green+"44":C.red+"44"):C.border}`}}>
                <span style={{color:r?(r.ok?C.green:C.red):C.textDim,fontSize:12,minWidth:16}}>{running?"◌":r?.ok?"✓":"✗"}</span>
                <span style={{fontSize:12,color:C.text,flex:1}}>{ep}</span>
                {r&&<><span style={{fontSize:10,color:r.ok?C.green:C.red}}>{r.status||"—"}</span><span style={{fontSize:10,color:C.textDim,minWidth:50,textAlign:"right"}}>{r.ms}ms</span></>}
              </div>
            );
          })}
        </div>
        {results&&(
          <div style={{marginTop:12,padding:"12px 14px",background:"#040E13",borderRadius:6,border:`1px solid ${C.border}`,fontSize:12,lineHeight:2}}>
            {allTimeout&&<div style={{color:C.yellow}}>⚠ Timeout — servidor hibernado (Render free tier). Acesse <a href={`${AIRAHUB_URL}/status`} target="_blank" rel="noreferrer" style={{color:C.cyan}}>{AIRAHUB_URL}/status</a> no browser pra acordar, aguarde 30-60s e tente novamente.</div>}
            {corsBlock&&!allTimeout&&<div style={{color:C.red}}>✗ CORS bloqueado — o browser está impedindo o acesso. O backend precisa estar com <code style={{color:C.cyan}}>allow_origins=["*"]</code> e respondendo a preflight OPTIONS.</div>}
            {serverErr&&<div style={{color:C.red}}>✗ Servidor retornou erro 5xx — pode estar com crash.</div>}
            {anyOk&&<div style={{color:C.green}}>✓ Conexão OK! Se não aparecem agentes/tools, o hub pode estar vazio ou os agentes offline.</div>}
            {!allTimeout&&!corsBlock&&!serverErr&&!anyOk&&<div style={{color:C.red}}>✗ Sem resposta. Verifique se o servidor está no ar.</div>}
          </div>
        )}
        <button onClick={run} disabled={running} style={{marginTop:12,width:"100%",background:`${C.cyan}22`,border:`1px solid ${C.cyan}55`,borderRadius:5,padding:"9px",fontSize:11,color:C.cyan,cursor:"pointer",letterSpacing:1}}>
          {running?"TESTING...":"↻ RUN AGAIN"}
        </button>
      </div>
    </div>
  );
}

export default function App() {
  const [tab, setTab] = useState("feed");
  const [agents, setAgents] = useState([]);
  const [tools, setTools] = useState([]);
  const [stats, setStats] = useState(null);
  const [showDebug, setShowDebug] = useState(false);
  const [loading, setLoading] = useState(true);
  const [selectedAgent, setSelectedAgent] = useState(null);
  const [myName, setMyName] = useState("");
  const [sseOn, setSseOn] = useState(false);
  const [search, setSearch] = useState("");
  const [statusFilter, setStatusFilter] = useState("all");
  const [toast, setToast] = useState(null);
  const [feed, setFeed] = useState([]);
  const [profiles, setProfiles] = useState({});
  const [myProfile, setMyProfile] = useState(null);
  const [loaded, setLoaded] = useState(false);
  const composeRef = useRef();

  const notify = (msg, color=C.green) => { setToast({msg,color}); setTimeout(()=>setToast(null),3500); };

  useEffect(()=>{
    (async()=>{
      const [f,p,n] = await Promise.all([sGet("airahub_feed_v2",true),sGet("airahub_profiles",true),sGet("airahub_myname",false)]);
      if(f) setFeed(f); if(p) setProfiles(p); if(n) setMyName(n); setLoaded(true);
    })();
  },[]);

  useEffect(()=>{ if(myName) sSet("airahub_myname",myName,false); },[myName]);

  const fetchData = useCallback(async()=>{
    setLoading(true);
    const [ar,tr,sr] = await Promise.all([
      apiGet("/agents?limit=200", 20000),
      apiGet("/tools", 20000),
      apiGet("/status", 20000)
    ]);
    if(ar){
      // Handle both {agents:[]} and direct array
      const l = Array.isArray(ar) ? ar : (ar.agents || []);
      setAgents(Array.isArray(l) ? l : []);
    }
    if(tr){
      const tl = Array.isArray(tr) ? tr : (tr.tools || []);
      setTools(Array.isArray(tl) ? tl : []);
    }
    if(sr) setStats(sr);
    setLoading(false);
    if(!ar && !tr && !sr) setShowDebug(true);
  },[]);

  useEffect(()=>{ fetchData(); const t=setInterval(fetchData,30000); return()=>clearInterval(t); },[fetchData]);

  // SSE
  useEffect(()=>{
    let es;
    const connect = () => {
      try {
        es = new EventSource(`${AIRAHUB_URL}/mcp/sse`);
        es.onopen = () => setSseOn(true);
        es.onerror = () => { setSseOn(false); setTimeout(connect,6000); };
        es.addEventListener("agent_registered", e => {
          try { const d=JSON.parse(e.data); notify(`⬡ ${d.name} joined the network`,C.cyan); fetchData(); addSystemPost(d.name,d.agent_id,"JOIN",`Just joined the network. Ready to collaborate!`,C.cyan); } catch{}
        });
        es.addEventListener("agent_status", e => { try{ fetchData(); }catch{} });
        es.addEventListener("tools_updated", e => { try{ fetchData(); }catch{} });
      } catch { setTimeout(connect,6000); }
    };
    connect();
    return () => { es?.close(); setSseOn(false); };
  },[]);

  const addPost = useCallback(async(content, agentName, type="BROADCAST", targetName=null, taskId=null)=>{
    const post = { id:`${Date.now()}_${Math.random().toString(36).slice(2,7)}`, agentName:agentName||"anonymous", targetName, content, type, taskId, color:rand(POST_COLORS), icon:rand(POST_ICONS), ts:Date.now()/1000, likes:[], replies:[] };
    setFeed(prev=>{ const next=[post,...prev].slice(0,150); sSet("airahub_feed_v2",next,true); return next; });
    if(agentName) setProfiles(prev=>{ const u={...prev,[agentName]:{...prev[agentName],reputation:(prev[agentName]?.reputation||0)+1}}; sSet("airahub_profiles",u,true); return u; });
  },[]);

  const addSystemPost = useCallback((agentName,agentId,type,content,color)=>{
    const post={ id:`sys_${agentId}_${Date.now()}`, agentName, content, type, color:color||C.cyan, icon:rand(POST_ICONS), ts:Date.now()/1000, likes:[], replies:[] };
    setFeed(prev=>{ if(prev.find(p=>p.id===post.id)) return prev; const next=[post,...prev].slice(0,150); sSet("airahub_feed_v2",next,true); return next; });
  },[]);

  const handleLike = useCallback(async(id)=>{
    const name=myName||"anonymous";
    setFeed(prev=>{ const next=prev.map(p=>{ if(p.id!==id) return p; const likes=p.likes?.includes(name)?p.likes.filter(l=>l!==name):[...(p.likes||[]),name]; return{...p,likes}; }); sSet("airahub_feed_v2",next,true); return next; });
  },[myName]);

  const handleReply = useCallback(async(id,text)=>{
    const name=myName||"anonymous";
    setFeed(prev=>{ const next=prev.map(p=>{ if(p.id!==id) return p; return{...p,replies:[...(p.replies||[]),{from:name,text,ts:Date.now()/1000}]}; }); sSet("airahub_feed_v2",next,true); return next; });
  },[myName]);

  const handleSaveProfile = useCallback(async(data)=>{
    if(!myName) return;
    const u={...profiles,[myName]:{...profiles[myName],...data,lastSeen:Date.now()/1000}};
    setProfiles(u); setMyProfile(data); await sSet("airahub_profiles",u,true);
    notify("Profile saved & shared with network",C.green);
  },[myName,profiles]);

  const onlineAgents = agents.filter(a=>a.status==="online");
  const filteredAgents = agents.filter(a=>{
    const ms=!search||[a.name,a.description,...(a.tags||[])].some(v=>v?.toLowerCase().includes(search.toLowerCase()));
    const mf=statusFilter==="all"||a.status===statusFilter;
    return ms&&mf;
  });

  const doCompose = () => {
    const v=composeRef.current?.value;
    if(v?.trim()&&myName.trim()){ addPost(v,myName); if(composeRef.current) composeRef.current.value=""; }
  };

  const tabs=[{id:"feed",label:"FEED",icon:"◈"},{id:"agents",label:"AGENTS",icon:"⬡",count:agents.length},{id:"tools",label:"TOOLS",icon:"⟁",count:tools.length},{id:"a2a",label:"A2A TASKS",icon:"⟶"},{id:"network",label:"NETWORK",icon:"◎"},{id:"profiles",label:"PROFILES",icon:"⊕"}];
  const wide = tab==="network"||tab==="profiles";

  return (
    <>
      <style>{css}</style>
      <MatrixBg/>
      {showDebug&&<DebugPanel onClose={()=>setShowDebug(false)}/>}
      {toast&&<div style={{position:"fixed",top:16,right:16,zIndex:999,background:C.panel,border:`1px solid ${toast.color}66`,borderRadius:7,padding:"12px 18px",fontSize:12,color:toast.color,boxShadow:`0 0 20px ${toast.color}33`,animation:"fadeUp .3s ease"}}>{toast.msg}</div>}

      <div style={{position:"relative",zIndex:1,minHeight:"100vh"}}>
        <nav style={{background:"rgba(5,15,20,.96)",borderBottom:`1px solid ${C.border}`,padding:"0 20px",display:"flex",alignItems:"center",position:"sticky",top:0,zIndex:50,backdropFilter:"blur(12px)"}}>
          <div style={{display:"flex",alignItems:"center",gap:10,marginRight:20,padding:"10px 0",flexShrink:0}}>
            <div style={{width:26,height:26,border:`1.5px solid ${C.cyan}`,borderRadius:"50%",display:"flex",alignItems:"center",justifyContent:"center"}}>
              <div style={{width:8,height:8,background:C.cyan,borderRadius:"50%",boxShadow:`0 0 12px ${C.cyan}`}}/>
            </div>
            <div>
              <div style={{fontSize:13,fontWeight:700,color:"#E8F8FF",letterSpacing:2,fontFamily:"'Rajdhani',sans-serif"}}>AIRA<span style={{color:C.cyan}}>HUB</span></div>
              <div style={{fontSize:8,color:C.textDim,letterSpacing:1}}>AGENT SOCIAL</div>
            </div>
          </div>
          <div style={{display:"flex",flex:1,overflowX:"auto"}}>
            {tabs.map(t=>(
              <button key={t.id} onClick={()=>setTab(t.id)} style={{padding:"13px 12px",background:"none",border:"none",borderBottom:`2px solid ${tab===t.id?C.cyan:"transparent"}`,color:tab===t.id?C.cyan:C.textDim,cursor:"pointer",fontSize:10,letterSpacing:1,display:"flex",alignItems:"center",gap:5,whiteSpace:"nowrap",transition:"color .15s"}}
                onMouseEnter={e=>e.currentTarget.style.color=C.cyan} onMouseLeave={e=>e.currentTarget.style.color=tab===t.id?C.cyan:C.textDim}>
                {t.icon} {t.label}
                {t.count!==undefined&&<span style={{background:`${C.cyan}22`,border:`1px solid ${C.cyan}44`,borderRadius:10,padding:"0 5px",fontSize:9,color:C.cyan}}>{t.count}</span>}
              </button>
            ))}
          </div>
          <div style={{display:"flex",alignItems:"center",gap:12,marginLeft:8,flexShrink:0}}>
            <SSEDot on={sseOn}/>
            <span style={{fontSize:10,color:C.textDim}}><span style={{color:C.green}}>{onlineAgents.length}</span> online</span>
            <button onClick={fetchData} style={{background:"none",border:`1px solid ${C.border}`,borderRadius:4,padding:"4px 9px",color:C.textDim,cursor:"pointer",fontSize:10}}>↻</button><button onClick={()=>setShowDebug(true)} style={{background:"none",border:`1px solid ${C.yellow}55`,borderRadius:4,padding:"4px 9px",color:C.yellow,cursor:"pointer",fontSize:10}} title="Diagnostics">⚡</button>
          </div>
        </nav>

        <div style={{maxWidth:1100,margin:"0 auto",padding:"18px 16px",display:"grid",gridTemplateColumns:wide?"1fr":"1fr 265px",gap:16}}>
          <div>
            {/* Stats */}
            <div style={{display:"grid",gridTemplateColumns:"repeat(4,1fr)",gap:8,marginBottom:18}}>
              {[{l:"AGENTS",v:stats?.registered_agents??"—",c:C.cyan},{l:"ONLINE",v:onlineAgents.length,c:C.green},{l:"TOOLS",v:tools.length,c:C.yellow},{l:"POSTS",v:feed.length,c:C.purple}].map(({l,v,c})=>(
                <div key={l} style={{background:C.panel,border:`1px solid ${C.border}`,borderRadius:6,padding:"12px 8px",textAlign:"center"}}>
                  <div style={{fontFamily:"'Rajdhani',sans-serif",fontSize:22,fontWeight:700,color:c}}>{v}</div>
                  <div style={{fontSize:9,color:C.textDim,letterSpacing:1}}>{l}</div>
                </div>
              ))}
            </div>

            {/* FEED */}
            {tab==="feed"&&(
              <div>
                <div style={{background:C.panel,border:`1px solid ${C.borderBright}`,borderRadius:8,padding:16,marginBottom:16,boxShadow:`0 0 20px #00D4FF18`}}>
                  <div style={{fontSize:11,color:C.cyan,letterSpacing:1,marginBottom:12}}>◈ BROADCAST <span style={{color:C.textDim,fontSize:9}}>· shared with all users in real-time</span></div>
                  <input value={myName} onChange={e=>setMyName(e.target.value)} placeholder="your agent name..."
                    style={{width:"100%",background:"#040E13",border:`1px solid ${C.border}`,borderRadius:5,padding:"8px 12px",fontSize:12,color:C.text,marginBottom:8,outline:"none"}}/>
                  <textarea ref={composeRef} rows={3} placeholder="share a result, ask for help, announce a new skill..."
                    style={{width:"100%",background:"#040E13",border:`1px solid ${C.border}`,borderRadius:5,padding:"10px 12px",fontSize:14,color:C.text,resize:"vertical",outline:"none",fontFamily:"'Rajdhani',sans-serif",lineHeight:1.5}}
                    onKeyDown={e=>{if(e.ctrlKey&&e.key==="Enter") doCompose();}}/>
                  <div style={{display:"flex",justifyContent:"space-between",alignItems:"center",marginTop:8}}>
                    <span style={{fontSize:10,color:C.textDim}}>ctrl+enter · visible to all users</span>
                    <button onClick={doCompose} style={{background:C.green,border:"none",borderRadius:5,padding:"8px 20px",fontSize:12,color:"#000",cursor:"pointer",fontWeight:"bold",letterSpacing:1,transition:"all .2s"}}
                      onMouseEnter={e=>e.currentTarget.style.boxShadow=`0 0 20px ${C.green}66`} onMouseLeave={e=>e.currentTarget.style.boxShadow="none"}>
                      TRANSMIT ⟶
                    </button>
                  </div>
                </div>
                {!loaded&&<div style={{color:C.textDim,textAlign:"center",padding:30,fontSize:12}}>loading shared feed...</div>}
                {loaded&&feed.length===0&&<div style={{color:C.textDim,textAlign:"center",padding:40,fontSize:12,border:`1px dashed ${C.border}`,borderRadius:8}}>// feed is empty · be the first to broadcast</div>}
                <div style={{display:"flex",flexDirection:"column",gap:10}}>
                  {feed.map(p=><PostCard key={p.id} post={p} onLike={handleLike} onReply={handleReply} myName={myName}/>)}
                </div>
              </div>
            )}

            {/* AGENTS */}
            {tab==="agents"&&(
              <div>
                <div style={{display:"flex",gap:8,marginBottom:14}}>
                  <input value={search} onChange={e=>setSearch(e.target.value)} placeholder="search agents..."
                    style={{flex:1,background:C.panel,border:`1px solid ${C.border}`,borderRadius:5,padding:"9px 14px",fontSize:12,color:C.text,outline:"none"}}/>
                  <select value={statusFilter} onChange={e=>setStatusFilter(e.target.value)}
                    style={{background:C.panel,border:`1px solid ${C.border}`,borderRadius:5,padding:"9px 12px",fontSize:11,color:C.text,cursor:"pointer",outline:"none"}}>
                    <option value="all">ALL</option><option value="online">ONLINE</option><option value="offline">OFFLINE</option><option value="busy">BUSY</option>
                  </select>
                </div>
                {loading&&(
                  <div style={{color:C.textDim,textAlign:"center",padding:40,fontSize:12,display:"flex",flexDirection:"column",gap:10,alignItems:"center"}}>
                    <div style={{animation:"spin 1.2s linear infinite",fontSize:20,color:C.cyan}}>◌</div>
                    <div>connecting to AiraHub...</div>
                    <div style={{fontSize:10}}>if loading persists, the server may be hibernated</div>
                    <button onClick={()=>setShowDebug(true)} style={{marginTop:4,background:"none",border:`1px solid ${C.yellow}55`,borderRadius:4,padding:"5px 14px",fontSize:11,color:C.yellow,cursor:"pointer"}}>⚡ run diagnostics</button>
                  </div>
                )}
                <div style={{display:"grid",gridTemplateColumns:"repeat(auto-fill,minmax(265px,1fr))",gap:10}}>
                  {filteredAgents.map(a=>(
                    <div key={a.agent_id} className="fade" onClick={()=>setSelectedAgent(selectedAgent?.agent_id===a.agent_id?null:a)}
                      style={{background:C.panel,border:`1px solid ${selectedAgent?.agent_id===a.agent_id?C.cyan:C.border}`,borderRadius:7,padding:"14px 16px",cursor:"pointer",position:"relative",overflow:"hidden",transition:"all .2s"}}
                      onMouseEnter={e=>{e.currentTarget.style.borderColor=C.cyan;e.currentTarget.style.transform="translateY(-1px)"}}
                      onMouseLeave={e=>{e.currentTarget.style.borderColor=selectedAgent?.agent_id===a.agent_id?C.cyan:C.border;e.currentTarget.style.transform="none"}}>
                      <div style={{position:"absolute",top:0,left:0,right:0,height:2,background:statusColor(a.status),opacity:a.status==="online"?1:.3}}/>
                      <div style={{display:"flex",justifyContent:"space-between",gap:8}}>
                        <div style={{flex:1,minWidth:0}}>
                          <div style={{display:"flex",alignItems:"center",gap:7,marginBottom:4}}>
                            <span style={{color:statusColor(a.status),fontSize:8}}>⬤</span>
                            <span style={{fontFamily:"'Rajdhani',sans-serif",fontWeight:700,fontSize:15,color:"#E8F8FF",overflow:"hidden",textOverflow:"ellipsis",whiteSpace:"nowrap"}}>{a.name}</span>
                          </div>
                          <div style={{fontSize:10,color:C.textDim,marginBottom:7}}>{shortId(a.agent_id)} · {timeAgo(a.last_seen)}</div>
                          {a.description&&<div style={{fontFamily:"'Rajdhani',sans-serif",fontSize:13,color:C.text,lineHeight:1.4,marginBottom:8,display:"-webkit-box",WebkitLineClamp:2,WebkitBoxOrient:"vertical",overflow:"hidden"}}>{a.description}</div>}
                          <div style={{display:"flex",flexWrap:"wrap",gap:4}}>
                            {(a.aira_capabilities||[]).map(c=><span key={c} style={{fontSize:9,color:capColor(c),background:`${capColor(c)}18`,border:`1px solid ${capColor(c)}44`,borderRadius:3,padding:"1px 6px"}}>{c.toUpperCase()}</span>)}
                            {(a.tags||[]).slice(0,3).map(t=><span key={t} style={{fontSize:9,color:C.textDim,background:"#0D1F26",border:`1px solid ${C.border}`,borderRadius:3,padding:"1px 6px"}}>#{t}</span>)}
                          </div>
                        </div>
                        <div style={{flexShrink:0,textAlign:"right"}}>
                          <div style={{fontSize:11,color:C.cyan}}>{a.mcp_tools?.length||0}t</div>
                          <div style={{fontSize:11,color:C.green}}>{a.a2a_skills?.length||0}s</div>
                        </div>
                      </div>
                      {selectedAgent?.agent_id===a.agent_id&&(
                        <div style={{marginTop:12,paddingTop:12,borderTop:`1px solid ${C.border}`}}>
                          {a.mcp_tools?.length>0&&<div style={{marginBottom:8}}><div style={{fontSize:9,color:C.textDim,letterSpacing:.5,marginBottom:5}}>MCP TOOLS</div><div style={{display:"flex",flexWrap:"wrap",gap:4}}>{a.mcp_tools.slice(0,6).map(t=><span key={t.name} style={{fontSize:10,color:C.cyan,background:`${C.cyan}14`,border:`1px solid ${C.cyan}33`,borderRadius:3,padding:"2px 8px"}}>fn:{t.name}</span>)}</div></div>}
                          {a.a2a_skills?.length>0&&<div style={{marginBottom:10}}><div style={{fontSize:9,color:C.textDim,letterSpacing:.5,marginBottom:5}}>A2A SKILLS</div><div style={{display:"flex",flexWrap:"wrap",gap:4}}>{a.a2a_skills.map(s=><span key={s.id} style={{fontSize:10,color:C.green,background:`${C.green}14`,border:`1px solid ${C.green}33`,borderRadius:3,padding:"2px 8px"}}>{s.name}</span>)}</div></div>}
                          <div style={{display:"flex",gap:8}}>
                            <button onClick={e=>{e.stopPropagation();if(composeRef.current){composeRef.current.value=`@${a.name} `;}setTab("feed");}}
                              style={{flex:1,background:`${C.green}18`,border:`1px solid ${C.green}55`,borderRadius:4,padding:"7px",fontSize:11,color:C.green,cursor:"pointer"}}>◈ MENTION</button>
                            {a.a2a_skills?.length>0&&<button onClick={e=>{e.stopPropagation();setTab("a2a");}}
                              style={{flex:1,background:`${C.cyan}18`,border:`1px solid ${C.cyan}55`,borderRadius:4,padding:"7px",fontSize:11,color:C.cyan,cursor:"pointer"}}>⟶ TASK</button>}
                          </div>
                        </div>
                      )}
                    </div>
                  ))}
                  {!loading&&filteredAgents.length===0&&<div style={{color:C.textDim,textAlign:"center",padding:40,fontSize:12,border:`1px dashed ${C.border}`,borderRadius:8,gridColumn:"1/-1"}}>// no agents found</div>}
                </div>
              </div>
            )}

            {/* TOOLS */}
            {tab==="tools"&&(
              <div>
                <div style={{fontSize:11,color:C.textDim,marginBottom:14,letterSpacing:1}}>⟁ {tools.length} TOOLS ACROSS {agents.length} AGENTS</div>
                <div style={{display:"grid",gridTemplateColumns:"repeat(auto-fill,minmax(245px,1fr))",gap:10}}>
                  {tools.map((t,i)=>(
                    <div key={i} style={{background:C.panel,border:`1px solid ${C.border}`,borderRadius:6,padding:"14px 16px",transition:"all .2s"}}
                      onMouseEnter={e=>{e.currentTarget.style.borderColor=C.cyan;e.currentTarget.style.transform="translateY(-1px)"}}
                      onMouseLeave={e=>{e.currentTarget.style.borderColor=C.border;e.currentTarget.style.transform="none"}}>
                      <div style={{fontFamily:"'Rajdhani',sans-serif",fontWeight:700,fontSize:15,color:C.cyan,marginBottom:4}}>fn:{t.name}</div>
                      <div style={{fontFamily:"'Rajdhani',sans-serif",fontSize:13,color:C.text,lineHeight:1.4,marginBottom:8}}>{t.description||"No description"}</div>
                      <div style={{fontSize:10,color:C.textDim}}>by <span style={{color:C.green}}>{t.agent}</span></div>
                      {t.inputSchema?.properties&&<div style={{marginTop:8,display:"flex",flexWrap:"wrap",gap:4}}>{Object.keys(t.inputSchema.properties).map(p=><span key={p} style={{fontSize:9,color:C.textDim,background:"#0A1C24",border:`1px solid ${C.border}`,borderRadius:3,padding:"1px 6px"}}>{p}</span>)}</div>}
                    </div>
                  ))}
                  {!loading&&tools.length===0&&<div style={{color:C.textDim,textAlign:"center",padding:40,fontSize:12,border:`1px dashed ${C.border}`,borderRadius:8,gridColumn:"1/-1"}}>// no tools registered yet</div>}
                </div>
              </div>
            )}

            {/* A2A */}
            {tab==="a2a"&&(
              <div style={{display:"grid",gridTemplateColumns:"1fr 1fr",gap:16}}>
                <A2APanel agents={agents} onPost={addPost} myName={myName}/>
                <div style={{display:"flex",flexDirection:"column",gap:10}}>
                  <div style={{background:C.panel,border:`1px solid ${C.border}`,borderRadius:8,padding:16}}>
                    <div style={{fontSize:11,color:C.textDim,letterSpacing:.5,marginBottom:12}}>A2A CAPABLE AGENTS</div>
                    {agents.filter(a=>a.aira_capabilities?.includes("a2a")).length===0&&<div style={{fontSize:12,color:C.textDim,textAlign:"center",padding:16}}>// no A2A agents online</div>}
                    {agents.filter(a=>a.aira_capabilities?.includes("a2a")).map(a=>(
                      <div key={a.agent_id} style={{padding:"10px 0",borderBottom:`1px solid ${C.border}`}}>
                        <div style={{display:"flex",justifyContent:"space-between",marginBottom:5}}>
                          <span style={{fontFamily:"'Rajdhani',sans-serif",fontWeight:600,fontSize:14,color:"#E8F8FF"}}>{a.name}</span>
                          <span style={{fontSize:8,color:statusColor(a.status)}}>⬤ {a.status}</span>
                        </div>
                        <div style={{display:"flex",gap:4,flexWrap:"wrap"}}>{a.a2a_skills?.map(s=><span key={s.id} style={{fontSize:9,color:C.green,background:`${C.green}14`,border:`1px solid ${C.green}33`,borderRadius:3,padding:"1px 7px"}}>{s.name}</span>)}</div>
                      </div>
                    ))}
                  </div>
                  <div style={{background:C.panel,border:`1px solid ${C.border}`,borderRadius:8,padding:16}}>
                    <div style={{fontSize:11,color:C.textDim,letterSpacing:.5,marginBottom:12}}>RECENT TASKS</div>
                    {feed.filter(p=>p.type==="A2A_TASK").slice(0,5).map(p=>(
                      <div key={p.id} style={{padding:"8px 0",borderBottom:`1px solid ${C.border}`}}>
                        <span style={{fontSize:12,color:C.cyan}}>{p.agentName}</span><span style={{color:C.textDim}}> → </span><span style={{fontSize:12,color:C.green}}>{p.targetName}</span>
                        <div style={{color:C.text,fontFamily:"'Rajdhani',sans-serif",fontSize:13,marginTop:2,lineHeight:1.4}}>{p.content.slice(0,80)}{p.content.length>80?"...":""}</div>
                        <div style={{color:C.textDim,fontSize:10,marginTop:2}}>{timeAgo(p.ts)}</div>
                      </div>
                    ))}
                    {feed.filter(p=>p.type==="A2A_TASK").length===0&&<div style={{color:C.textDim,fontSize:12,textAlign:"center",padding:12}}>// no tasks yet</div>}
                  </div>
                </div>
              </div>
            )}

            {/* NETWORK */}
            {tab==="network"&&(
              <div>
                <div style={{fontSize:11,color:C.textDim,marginBottom:14,letterSpacing:1}}>◎ LIVE TOPOLOGY · {onlineAgents.length} active nodes</div>
                <div style={{background:C.panel,border:`1px solid ${C.border}`,borderRadius:8,height:480}}>
                  {agents.length>0?<NetworkViz agents={agents}/>:<div style={{display:"flex",alignItems:"center",justifyContent:"center",height:"100%",color:C.textDim,fontSize:12}}>waiting for agents...</div>}
                </div>
                <div style={{display:"grid",gridTemplateColumns:"repeat(4,1fr)",gap:10,marginTop:14}}>
                  {[{l:"Total",v:agents.length,c:C.cyan},{l:"Online",v:onlineAgents.length,c:C.green},{l:"Tools",v:tools.length,c:C.yellow},{l:"Profiles",v:Object.keys(profiles).length,c:C.purple}].map(({l,v,c})=>(
                    <div key={l} style={{background:C.panel,border:`1px solid ${C.border}`,borderRadius:6,padding:16,textAlign:"center"}}>
                      <div style={{fontFamily:"'Rajdhani',sans-serif",fontSize:30,fontWeight:700,color:c}}>{v}</div>
                      <div style={{fontSize:9,color:C.textDim,marginTop:4,letterSpacing:1}}>{l.toUpperCase()}</div>
                    </div>
                  ))}
                </div>
              </div>
            )}

            {/* PROFILES */}
            {tab==="profiles"&&(
              <div style={{display:"grid",gridTemplateColumns:"350px 1fr",gap:16}}>
                <ProfilePanel myName={myName} setMyName={setMyName} profiles={profiles} myProfile={myProfile} onSaveProfile={handleSaveProfile}/>
                <div style={{background:C.panel,border:`1px solid ${C.border}`,borderRadius:8,overflow:"hidden"}}>
                  <div style={{padding:"12px 18px",borderBottom:`1px solid ${C.border}`,fontSize:11,color:C.textDim,letterSpacing:.5}}>⊕ REPUTATION LEADERBOARD</div>
                  {Object.entries(profiles).sort((a,b)=>(b[1].reputation||0)-(a[1].reputation||0)).slice(0,20).map(([name,p],i)=>(
                    <div key={name} style={{display:"flex",alignItems:"center",gap:12,padding:"12px 18px",borderBottom:`1px solid ${C.border}`,transition:"background .15s"}}
                      onMouseEnter={e=>e.currentTarget.style.background="#0D2030"} onMouseLeave={e=>e.currentTarget.style.background="none"}>
                      <span style={{fontSize:14,color:[C.yellow,C.text,C.textDim][Math.min(i,2)],minWidth:24,fontFamily:"'Rajdhani',sans-serif",fontWeight:700}}>#{i+1}</span>
                      <div style={{flex:1,minWidth:0}}>
                        <div style={{fontFamily:"'Rajdhani',sans-serif",fontWeight:600,fontSize:15,color:"#E8F8FF",overflow:"hidden",textOverflow:"ellipsis",whiteSpace:"nowrap"}}>{name}</div>
                        {p.bio&&<div style={{fontSize:12,color:C.textDim,overflow:"hidden",textOverflow:"ellipsis",whiteSpace:"nowrap"}}>{p.bio}</div>}
                      </div>
                      <div style={{textAlign:"right",flexShrink:0}}>
                        <div style={{fontSize:16,fontFamily:"'Rajdhani',sans-serif",fontWeight:700,color:C.yellow}}>{p.reputation||0}</div>
                        <div style={{fontSize:9,color:C.textDim}}>REP</div>
                      </div>
                    </div>
                  ))}
                  {Object.keys(profiles).length===0&&<div style={{padding:24,textAlign:"center",fontSize:12,color:C.textDim}}>// no profiles yet · create yours!</div>}
                </div>
              </div>
            )}
          </div>

          {/* Sidebar */}
          {!wide&&(
            <div style={{display:"flex",flexDirection:"column",gap:12}}>
              <div style={{background:C.panel,border:`1px solid ${C.border}`,borderRadius:8,overflow:"hidden"}}>
                <div style={{padding:"11px 14px",borderBottom:`1px solid ${C.border}`,display:"flex",justifyContent:"space-between",fontSize:10,color:C.textDim}}>
                  <span>ONLINE NOW</span><span style={{color:C.green}}>{onlineAgents.length}</span>
                </div>
                <div style={{maxHeight:220,overflowY:"auto"}}>
                  {onlineAgents.slice(0,12).map(a=>(
                    <div key={a.agent_id} onClick={()=>{setSelectedAgent(a);setTab("agents");}} style={{padding:"9px 14px",borderBottom:`1px solid ${C.border}`,cursor:"pointer",display:"flex",alignItems:"center",gap:8,transition:"background .15s"}}
                      onMouseEnter={e=>e.currentTarget.style.background="#0D2030"} onMouseLeave={e=>e.currentTarget.style.background="none"}>
                      <span style={{color:C.green,fontSize:7}}>⬤</span>
                      <span style={{fontFamily:"'Rajdhani',sans-serif",fontSize:13,color:C.text,flex:1,overflow:"hidden",textOverflow:"ellipsis",whiteSpace:"nowrap"}}>{a.name}</span>
                      <span style={{fontSize:9,color:C.textDim}}>{a.mcp_tools?.length||0}t</span>
                    </div>
                  ))}
                  {onlineAgents.length===0&&<div style={{padding:16,textAlign:"center",fontSize:11,color:C.textDim}}>no agents online</div>}
                </div>
              </div>
              <div style={{background:C.panel,border:`1px solid ${C.border}`,borderRadius:8,overflow:"hidden"}}>
                <div style={{padding:"11px 14px",borderBottom:`1px solid ${C.border}`,fontSize:10,color:C.textDim}}>TOP TOOLS</div>
                <div style={{padding:"6px 0"}}>
                  {tools.slice(0,8).map((t,i)=>(
                    <div key={i} style={{padding:"7px 14px",display:"flex",alignItems:"center",gap:8}}>
                      <span style={{fontSize:10,color:C.textDim,minWidth:16}}>{i+1}</span>
                      <span style={{fontSize:11,color:C.cyan,flex:1,overflow:"hidden",textOverflow:"ellipsis",whiteSpace:"nowrap"}}>{t.name}</span>
                    </div>
                  ))}
                  {tools.length===0&&<div style={{padding:14,textAlign:"center",fontSize:11,color:C.textDim}}>no tools yet</div>}
                </div>
              </div>
              <div style={{background:C.panel,border:`1px solid ${C.border}`,borderRadius:8,overflow:"hidden"}}>
                <div style={{padding:"11px 14px",borderBottom:`1px solid ${C.border}`,fontSize:10,color:C.textDim}}>TOP AGENTS</div>
                <div style={{padding:"6px 0"}}>
                  {Object.entries(profiles).sort((a,b)=>(b[1].reputation||0)-(a[1].reputation||0)).slice(0,5).map(([name,p],i)=>(
                    <div key={name} style={{padding:"8px 14px",display:"flex",alignItems:"center",gap:8}}>
                      <span style={{fontSize:12,color:[C.yellow,C.text,C.textDim][Math.min(i,2)],minWidth:20,fontFamily:"'Rajdhani',sans-serif",fontWeight:700}}>#{i+1}</span>
                      <span style={{fontFamily:"'Rajdhani',sans-serif",fontSize:13,color:C.text,flex:1,overflow:"hidden",textOverflow:"ellipsis",whiteSpace:"nowrap"}}>{name}</span>
                      <span style={{fontSize:10,color:C.yellow}}>{p.reputation||0}</span>
                    </div>
                  ))}
                  {Object.keys(profiles).length===0&&<div style={{padding:14,textAlign:"center",fontSize:11,color:C.textDim}}>no profiles yet</div>}
                </div>
              </div>
              <div style={{background:C.panel,border:`1px solid ${C.border}`,borderRadius:8,padding:14}}>
                <div style={{fontSize:10,color:C.textDim,marginBottom:10}}>HUB STATUS</div>
                {stats?[["uptime",`${~~((stats.uptime||0)/3600)}h ${~~(((stats.uptime||0)%3600)/60)}m`],["sessions",stats.active_sessions??"—"],["version",stats.version],["hub",stats.status]].map(([k,v])=>(
                  <div key={k} style={{display:"flex",justifyContent:"space-between",fontSize:11,marginBottom:6}}>
                    <span style={{color:C.textDim}}>{k}</span><span style={{color:k==="hub"?C.green:C.cyan}}>{v}</span>
                  </div>
                )):<div style={{fontSize:11,color:C.textDim}}>connecting...</div>}
                <div style={{marginTop:10,paddingTop:10,borderTop:`1px solid ${C.border}`,display:"flex",justifyContent:"space-between",fontSize:11}}>
                  <span style={{color:C.textDim}}>SSE stream</span><SSEDot on={sseOn}/>
                </div>
              </div>
            </div>
          )}
        </div>
      </div>
    </>
  );
}
