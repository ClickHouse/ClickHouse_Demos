type Point={x:string;y:number;branch?:string};
export default function RetailSeries({points,label,unit='SGD'}:{points:Point[];label:string;unit?:string}){
 if(!points.length)return <p className="retail-empty">Waiting for the next completed sales batch. Refresh after the writer reports progress.</p>;
 const times=[...new Set(points.map(p=>p.x))].sort();const max=Math.max(1,...points.map(p=>p.y));
 const branches=[...new Set(points.map(p=>p.branch??'applied'))];
 const colors=['#067a68','#8662cb'];
 return <div className="retail-series"><svg viewBox="0 0 900 240" role="img" aria-label={label}>
 {[0,.5,1].map(f=><g key={f}><line x1="65" x2="875" y1={205-f*170} y2={205-f*170} stroke="#dfe3e6"/><text x="55" y={210-f*170} textAnchor="end" fontSize="12">{(max*f).toLocaleString(undefined,{maximumFractionDigits:1})}</text></g>)}
 {branches.map((branch,i)=>{const selected=points.filter(p=>(p.branch??'applied')===branch);const coords=selected.map(p=>`${65+times.indexOf(p.x)*810/Math.max(1,times.length-1)},${205-p.y/max*170}`).join(' ');return <g key={branch}><polyline fill="none" stroke={colors[i%2]} strokeWidth="3" strokeDasharray={branch==='comparison'?'7 4':undefined} points={coords}/>{selected.map((p,n)=><circle key={n} cx={65+times.indexOf(p.x)*810/Math.max(1,times.length-1)} cy={205-p.y/max*170} r="3" fill={colors[i%2]}/>)}</g>})}
 <text x="65" y="230" fontSize="12">{times[0]?.slice(11,16)} UTC</text><text x="875" y="230" textAnchor="end" fontSize="12">{times.at(-1)?.slice(11,16)} UTC</text><text x="65" y="20" fontSize="12">{unit}</text></svg>
 <div className="retail-legend"><span>● Applied price</span>{branches.includes('comparison')&&<span>┄ Unchanged price</span>}</div></div>;
}
