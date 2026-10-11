/** Round fractional minor units with decimal digits, never binary float multiplication. */
export function formatMoney(value:number|string):string{
  if(typeof value==='number'&&!Number.isSafeInteger(value))return 'Unavailable';
  const match=String(value).match(/^(-?)(\d+)(?:\.(\d+))?$/);
  if(!match)return 'Unavailable';
  let cents=BigInt(match[2]);
  if(match[3]&&match[3][0]>='5')cents+=1n;
  const sign=match[1]&&cents>0n?'-':'';
  return `SGD ${sign}${(cents/100n).toLocaleString('en-US')}.${String(cents%100n).padStart(2,'0')}`;
}
export default function Money({value}:{value:number|string}){return <span>{formatMoney(value)}</span>;}
export function changePercent(proposed:number,current:number){
  const bps=BigInt(proposed-current)*10000n/BigInt(current);
  const absolute=bps<0n?-bps:bps;
  return `${bps<0n?'−':bps>0n?'+':''}${absolute/100n}.${String(absolute%100n).padStart(2,'0')}%`;
}
