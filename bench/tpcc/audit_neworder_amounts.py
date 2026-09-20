#!/usr/bin/env python3
"""Read-only diagnostic of one retained HammerDB district; not a keeper gate.

Arguments: port, unique output label, optional user, optional database.
Outputs a new /tmp/bicdb-neworder-amount-LABEL.json file. The SQL is limited
to warehouse 1, district 1, and post-seed orders. Exit zero means the
diagnostic completed; inspect mismatch and missing counts for correctness.
The formula is the retained HammerDB procedure formula, not a declaration
of TPC-C compliance. Requires psycopg2 and a quiescent local SQL endpoint.
"""
import json,sys,time
from decimal import Decimal,ROUND_HALF_UP
import psycopg2
port=int(sys.argv[1]); label=sys.argv[2]
c=psycopg2.connect(host='127.0.0.1',port=port,user=(sys.argv[3] if len(sys.argv)>3 else 'bicdb'),password='x',dbname=(sys.argv[4] if len(sys.argv)>4 else 'bicdb'))
c.autocommit=True
q=c.cursor()
def rows(sql):
 q.execute(sql); cols=[x or [] for x in q.fetchone()]; assert len(set(map(len,cols)))==1; return zip(*cols)
prices=dict(rows('select array_agg(i_id),array_agg(i_price) from item'))
customers=dict(rows('select array_agg(c_id),array_agg(c_discount) from customer where c_w_id=1 and c_d_id=1'))
orders=dict(rows('select array_agg(o_id),array_agg(o_c_id) from orders where o_w_id=1 and o_d_id=1 and o_id>3000'))
q.execute('select w_tax from warehouse where w_id=1'); wt=Decimal(str(q.fetchone()[0]))
q.execute('select d_tax from district where d_w_id=1 and d_id=1'); dt=Decimal(str(q.fetchone()[0]))
count=0; mismatches=0; missing=0; missing_item=0; null_amount=0; large_delta=0; examples=[]; large_examples=[]; missing_examples=[]
for oid,num,iid,quantity,amount in rows('select array_agg(ol_o_id),array_agg(ol_number),array_agg(ol_i_id),array_agg(ol_quantity),array_agg(ol_amount) from order_line where ol_w_id=1 and ol_d_id=1 and ol_o_id>3000'):
 count+=1
 if iid not in prices or amount is None:
  missing+=1; missing_item+=int(iid not in prices); null_amount+=int(amount is None)
  if len(missing_examples)<10:missing_examples.append(dict(order=oid,line=num,item=iid,quantity=quantity,amount=amount))
  continue
 expected=(Decimal(str(prices[iid]))*Decimal(str(quantity))*(1+wt+dt)*(1-Decimal(str(customers[orders[oid]])))).quantize(Decimal('0.01'),rounding=ROUND_HALF_UP)
 if Decimal(str(amount))!=expected:
  mismatches+=1
  if abs(Decimal(str(amount))-expected)>Decimal('0.01'):
   large_delta+=1
   if len(large_examples)<10:large_examples.append(dict(order=oid,line=num,item=iid,quantity=quantity,amount=amount,expected=expected))
  if len(examples)<10:examples.append(dict(order=oid,line=num,item=iid,quantity=quantity,amount=amount,expected=expected))
c.close()
r=dict(label=label,scope='warehouse 1 district 1 post-seed line amount versus the retained HammerDB procedure formula',lines=count,mismatches=mismatches,missing=missing,missing_item=missing_item,null_amount=null_amount,large_delta=large_delta,examples=examples,large_examples=large_examples,missing_examples=missing_examples)
with open('/tmp/bicdb-neworder-amount-'+label+'.json','x') as f:json.dump(r,f,default=str,indent=2)
print(json.dumps(r,default=str))
