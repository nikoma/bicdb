import{MAX96,UNITS_LIMIT,toUnits}from'./exact-units-oracle.mjs';import{ratioOutcome}from'./ratio-representable-oracle.mjs';
export function ratioRepresentableVectors(){const rows=[],add=(args,label)=>rows.push({id:rows.length+1,operation:'native_ratio_representable_units_value',args,label,...ratioOutcome(args)}),M=MAX96,Q=10n**28n;
 for(const delta of[-2n,-1n,0n,1n,2n])add([(M*Q+Q/2n+delta).toString(),'1',0,28],'exact opposite sides of overflowing half-even midpoint');
 for(const tail of[0n,1n,49n,50n,51n,99n])add([(M*100n+tail).toString(),'1',0,2],'native MAX plusfraction; value beyondMAX can still once-round toMAX');
 for(const C of['0','1',M.toString(),(UNITS_LIMIT-1n).toString()])for(const[D,p,s]of[['1',0,0],['1',28,0],['1',0,28],[(UNITS_LIMIT-1n).toString(),28,28]])add([C,D,p,s],'bounded fullscale/wide/zero domains');
 add([((M+1n)/2n*Q).toString(),(Q/2n).toString(),28,28],'rounded half-quantity receipt cost yields unrepresentable derived rate');
 add([M.toString(),'1',0,0],'commercial input MAX remains representable independently');
 for(const args of[['0','0',0,0],['0','0',28,28],['-1','1',0,0],['01','1',0,0],['1','01',0,0],[' 1','1',0,0],['1.0','1',0,0],['１','1',0,0],[(UNITS_LIMIT).toString(),'1',0,0],['0','1',-1,0],['0','1',29,0],['0','1',0,29],['0','1',true,0],['0','1','0',0],['0','1',0.5,0],['0','1',0],['0','1',0,0,0],null])add(args,'invalid input must ERROR rather than returnfalse');
 return rows;}
export const rawRatioIntKindVectors=[{raw:'["1","1",0.0,0]',error:'strict_Int64_kind'},{raw:'["1","1",0,0e0]',error:'strict_Int64_kind'}];
