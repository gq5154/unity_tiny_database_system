// 
// MinBase
// Copyright (c) Casablanca Sistemas S.A. de C.V.
// Jorge Reyna Tamez.
// 2014-02-12
//

using System;
using System.IO;


namespace gBase{

   public delegate string mpIndexKey(Table obj);

   internal class IdxPos{
      public const byte coIxFound    = 1;
      public const byte coIxMatch    = 2;
      public const byte coIxBigger   = 3;
      public const byte coIxSmaller  = 4;

      public IdxPage Page;
      public long PageAdr;
      public long RowAdr;
      public ushort Row;
      public byte Result;
      public bool OnPage;
   }


   internal class IdxRow{
      internal string Key;
      internal long Address;
   }

   internal class IdxPage : Record{
      public const byte coOrder = 8;
      public const ushort coMin = coOrder/2;

      internal long Parent;
      internal long Previous;
      internal long Next;
      internal ushort Rows;
      private ushort KeyLength;
      internal IdxRow[] Table;

      public override void OnLoad(BinaryReader rd){
         Parent    = rd.ReadInt64();
         Previous  = rd.ReadInt64();
         Next      = rd.ReadInt64();
         Rows      = rd.ReadUInt16();
         KeyLength = rd.ReadUInt16();
         for(ushort i=0;i<coOrder;i++){
            Table[i].Key     = ReadFix(rd,KeyLength);
            Table[i].Address = rd.ReadInt64();
         }
      }
      public override void OnSave(BinaryWriter wr){
         wr.Write(Parent);
         wr.Write(Previous);
         wr.Write(Next);
         wr.Write(Rows);
         wr.Write(KeyLength);
         for(ushort i=0;i<coOrder;i++){
            WriteFix(wr,Table[i].Key,KeyLength);
            wr.Write(Table[i].Address);
         }
      }

      public IdxPage(long pan,long pvn,long nxn,ushort kl){
         Parent    = pan;
         Previous  = pvn;
         Next      = nxn;
         Table     = new IdxRow[coOrder];
         for(ushort i=0;i<coOrder;i++){
            Table[i] = new IdxRow();
            Table[i].Key     = "";
            Table[i].Address = 0;
         }
         KeyLength = kl;
      }

      public void First(IdxPos pos){
         pos.Row     = 0;
         pos.RowAdr = Table[0].Address;
      }

      public void Last(IdxPos pos){
         pos.Row    = (ushort)(Rows-1);
         pos.RowAdr = Table[pos.Row].Address;
      }

      public void Scan(string key,IdxPos pos){
         ushort k,b;
         int c,t;

         k = 0;
         c = 0;
         b = 0;
         t = (ushort)(Rows-1);

         while(t>=b){
            k = (ushort)((t+b)/2);
            c = string.Compare(Table[k].Key,key);
            if(c==0){
               b = k;
               while(b>0){
                  b--;
                  c = string.Compare(Table[b].Key,key);
                  if(c!=0) break;
                  k = b;
               }
               pos.Result = IdxPos.coIxFound;
               pos.RowAdr = Table[k].Address;
               pos.Row    = k;
               return;
            }else if(c>0){
               t = k-1;
            }else{
               b = (ushort)(k+1);
            }
         }

         if(c>0){
            pos.Result  = IdxPos.coIxBigger;
         }else{
            if(k<Rows-1){
               k++;
               pos.Result = IdxPos.coIxBigger;
            }else{
               pos.Result  = IdxPos.coIxSmaller;
            }
         }
         pos.RowAdr = Table[k].Address;
         pos.Row    = k;
      }
      public void Match(string key,long adr,IdxPos pos){
         for(ushort k=pos.Row;k<Rows;k++){
            if(string.Compare(Table[k].Key,key)!=0){
               pos.Result = IdxPos.coIxSmaller;
               return;
            }
            if(Table[k].Address==adr){
               pos.RowAdr = adr;
               pos.Row    = k;
               pos.Result = IdxPos.coIxMatch;
               return;
            }
         }
         pos.PageAdr = Next;
         pos.Row     = 0;
      }
      public long Jump(long jmp,IdxPos pos){
         long a;
         if(jmp>0L){
            if(pos.OnPage){
               pos.OnPage = false;
               if(pos.Row+jmp<Rows){
                  pos.Row    += (ushort)jmp;
                  pos.RowAdr  = Table[pos.Row].Address;
                  return 0;
               }
               a = (Rows-pos.Row-1);
            }else{
               if(jmp<=Rows){
                  pos.Row     = (ushort)(jmp-1);
                  pos.RowAdr = Table[pos.Row].Address;
                  return 0;
               }
               a = Rows;
            }
            pos.PageAdr = Next;
         }else{
            if(pos.OnPage){
               pos.OnPage = false;
               if(pos.Row + jmp >= 0){
                  pos.Row = (ushort)(pos.Row+jmp);
                  pos.RowAdr = Table[pos.Row].Address;
                  return 0;
               }
               a = -pos.Row;
            }else{
               a = -Rows;
               if(jmp>=a){
                  pos.Row = (ushort)(Rows+jmp);
                  pos.RowAdr = Table[pos.Row].Address;
                  return 0;
               }
            }
            pos.PageAdr = Previous;
         }
         return jmp-a;
      }

      public long Position(IdxPos pos){         
         pos.PageAdr = Previous;
         if(pos.OnPage){
            pos.OnPage = false;
            return pos.Row;
         }
         return Rows;
      }

      public void LinkChild(DataFile fle,long chadr,ushort kl,int pgsz){
         IdxPage ch = new IdxPage(0,0,0,kl);
         fle.Read(ch,chadr,pgsz);
         ch.Parent = this.Buffer.Position;
         fle.Write(ch);
      }

      public void LinkParent(DataFile fle,ushort kl,int pgsz){
         if(Parent==0) return;
         IdxPage pa  = new IdxPage(0,0,0,kl);
         long adr = this.Buffer.Position;
         string key = Table[Rows-1].Key;
         fle.Read(pa,Parent,pgsz);
         for(ushort i=0;i<pa.Rows;i++){
            if(pa.Table[i].Address==adr){
               pa.Table[i].Key = key;
               fle.Write(pa);
               if(i==pa.Rows-1) pa.LinkParent(fle,kl,pgsz);
               return;
            }
         }
         throw new System.InvalidOperationException("MinBase: Index file "+fle.LoFile.Name+" corrupted: tying to find record by address");
      }

      public void KeyOnParent(DataFile fle,ushort kl,int pgsz,IdxPos pos){
         IdxPage pa=new IdxPage(0,0,0,kl);
         long adr=this.Buffer.Position;
         fle.Read(pa,Parent,pgsz);
         for(ushort i=0;i<pa.Rows;i++){
            if(pa.Table[i].Address==adr){
               pos.Page=pa;
               pos.Row=i;
               return;
            }
         }
         throw new System.InvalidOperationException("MinBase: Index file "+fle.LoFile.Name+" corrupted: tying to find record by address");
      }

      public void Add(DataFile fle,string key,long adr,bool branch,IdxRoot root,IdxPos pos){
         int i;
         ushort j,mx,p,q,m,dx;
         int pgsz = root.PageSize;
         ushort kyln = root.KeyLength;
         ushort row  = pos.Row;

         if(pos.Result==IdxPos.coIxSmaller) row++;

         if(Rows<coOrder){
            for(i=Rows;i>row;i--){
               Table[i].Key     = Table[i-1].Key;
               Table[i].Address = Table[i-1].Address;
            }
            Table[row].Key     = key;
            Table[row].Address = adr;
            Rows++;
            if(row==Rows-1) LinkParent(fle,kyln,pgsz);
            if(root.Root==0){
               fle.Append(this,pgsz);
               root.Levels = 1;
               root.Root = root.First = root.Last = this.Buffer.Position;
            }else{
               fle.Write(this);
               if(branch) LinkChild(fle,adr,kyln,pgsz);
            }
            fle.Write(root);
            return;
         }

         IdxPage si = new IdxPage(0,0,0,kyln);
         if(Previous>0){
            fle.Read(si,Previous,root.PageSize);
            if(si.Rows<coOrder){
               dx = (ushort)((coOrder-si.Rows)/2);
               if(dx==0) dx = 1;
               if(row<dx){
                  p = (ushort)(si.Rows+row);
                  q = coOrder+1;
                  if(dx==1){
                     si.Table[si.Rows].Key     = key;
                     si.Table[si.Rows].Address = adr;
                     si.Rows++;
                     fle.Write(si);
                     si.LinkParent(fle,kyln,pgsz);
                     if(branch) si.LinkChild(fle,adr,kyln,pgsz);
                     return;
                  }
               }else{
                  p = coOrder+1;
                  q = (ushort)(row-dx);
               }
               for(i=0;i<dx;i++){
                  if(si.Rows==p){
                     si.Table[si.Rows].Key     = key;
                     si.Table[si.Rows].Address = adr;
                     si.Rows++;
                     if(branch) si.LinkChild(fle,adr,kyln,pgsz);
                     dx--;
                     if(dx==i) break;
                  }
                  si.Table[si.Rows].Key     = Table[i].Key;
                  si.Table[si.Rows].Address = Table[i].Address;
                  si.Rows++;
                  if(branch) si.LinkChild(fle,Table[i].Address,kyln,pgsz);
               }
               si.LinkParent(fle,kyln,pgsz);
               fle.Write(si);
               mx = (ushort)(coOrder-dx);
               j  = dx;
               m  = 0;
               for(i=0;i<mx;i++){
                  if(m==q){
                     Table[m].Key     = key;
                     Table[m].Address = adr;
                     if(branch) LinkChild(fle,adr,kyln,pgsz);
                     m++;
                     dx--;
                     if(m==j) break;
                  }
                  Table[m].Key     = Table[j].Key;
                  Table[m].Address = Table[j].Address;
                  m++;
                  j++;
               }
               Rows -= dx;
               if(q==Rows){
                  Table[q].Key     = key;
                  Table[q].Address = adr;
                  Rows++;
                  if(branch) LinkChild(fle,adr,kyln,pgsz);
               }
               if(q==Rows-1) LinkParent(fle,kyln,pgsz);
               fle.Write(this);
               fle.Write(root);
               return;
            }
         }

         if(Next>0){
            fle.Read(si,Next,pgsz);
            if(si.Rows<coOrder){
               dx = (ushort)((coOrder-si.Rows)/2);
               if(dx==0) dx = 1;
               mx = (ushort)(Rows-dx);
               if(row>mx){
                  p = (ushort)(row-mx-1);
               }else{
                  p = coOrder+1;
               }
               m = (ushort)(si.Rows+dx-1);
               j = (ushort)(m-dx);
               for(i=0;i<si.Rows;i++){
                  si.Table[m].Key     = si.Table[j].Key;
                  si.Table[m].Address = si.Table[j].Address;
                  m--;
                  j--;
               }
               si.Rows += dx;
               m = (ushort)(dx-1);
               j = coOrder-1;
               for(i=0;i<dx;i++){
                  if(m==p){
                     si.Table[m].Key = key;
                     si.Table[m].Address = adr;
                     if(branch) si.LinkChild(fle,adr,kyln,pgsz);
                     dx--;
                     if(m==0) break;
                     m--;
                  }
                  si.Table[m].Key     = Table[j].Key;
                  si.Table[m].Address = Table[j].Address;
                  if(branch) si.LinkChild(fle,Table[j].Address,kyln,pgsz);
                  m--;
                  j--;
               }
               Rows -= dx;
               if(p==coOrder+1){
                  for(i=Rows;i>row;i--){
                     Table[i].Key     = Table[i-1].Key;
                     Table[i].Address = Table[i-1].Address;
                  }
                  Table[row].Key = key;
                  Table[row].Address = adr;
                  if(branch) LinkChild(fle,adr,kyln,pgsz);
                  Rows++;
               }
               LinkParent(fle,kyln,pgsz);
               fle.Write(this);
               fle.Write(si);
               fle.Write(root);
               return;
            }
         }

         si = new IdxPage(Parent,this.Buffer.Position,Next,kyln);
         IdxPage pa;
         ushort mv = si.Rows = Rows = coMin;
         ushort hp,wr;
         long thad;
         string skey;
         if(row>=coMin){
            hp   = (ushort)(row-coMin);
         }else{
            hp = coOrder;
         }
         wr = 0;
         for(i=0;i<coMin;i++){
            if(i==hp){
               si.Table[wr].Key     = key;
               si.Table[wr].Address = adr;
               wr++;
               si.Rows++;
            }
            si.Table[wr].Key     = Table[mv].Key;
            si.Table[wr].Address = Table[mv].Address;
            wr++;
            mv++;
         }
         if(hp==coMin){
            si.Table[coMin].Key     = key;
            si.Table[coMin].Address = adr;
            si.Rows++;
         }
         fle.Append(si,pgsz);
         Next = si.Buffer.Position;

         if(branch) for(i=0;i<si.Rows;i++) si.LinkChild(fle,si.Table[i].Address,kyln,pgsz);

         if(si.Rows==Rows){
            for(i=Rows;i>row;i--){
               Table[i].Key     = Table[i-1].Key;
               Table[i].Address = Table[i-1].Address;
            }
            Table[row].Key     = key;
            Table[row].Address = adr;
            if(branch) LinkChild(fle,adr,kyln,pgsz);
            Rows++;
         }

         if(si.Next==0){
            if(!branch) root.Last = Next;
            if(Parent==0){
               pa = new IdxPage(0,0,0,kyln);
               pa.Rows = 2;
               pa.Table[0].Key     = Table[Rows-1].Key;
               pa.Table[0].Address = si.Previous;
               pa.Table[1].Key     = si.Table[si.Rows-1].Key;
               pa.Table[1].Address = Next;
               fle.Append(pa,pgsz);
               Parent = si.Parent = pa.Buffer.Position;
               fle.Write(this);
               fle.Write(si);
               root.Root = Parent;
               root.Levels++;
               fle.Write(root);
               return;
            }
         }else{
            IdxPage ss = new IdxPage(0,0,0,kyln);
            fle.Read(ss,si.Next,pgsz);
            ss.Previous = Next;
            fle.Write(ss);
         }

         LinkParent(fle,kyln,pgsz);
         fle.Write(this);

         skey = si.Table[si.Rows-1].Key;
         thad = this.Buffer.Position;
         pa   = new IdxPage(0,0,0,kyln);
         fle.Read(pa,Parent,pgsz);
         for(j=0;j<pa.Rows;j++){
            if(pa.Table[j].Address==thad){
               pos.Row    = j;
               pos.Result = IdxPos.coIxSmaller;
               pa.Add(fle,skey,Next,true,root,pos);
               return;
            }
         }
         throw new System.InvalidOperationException("MinBase: Index file "+fle.LoFile.Name+" corrupted, trying to add key.");

      }

      public void Remove(DataFile fle,string key,long adr,bool branch,IdxRoot root,IdxPos pos){
         ushort i;
         ushort row  = pos.Row;
         int pgsz = root.PageSize;
         ushort kyln = root.KeyLength;

         if(Rows>coMin || Parent==0){
            if(Rows==1){
               root.Root = root.First = root.Last = 0;
               root.Levels = 0;
               fle.Write(root);
               return;
            }
            Rows--;
            if(row==Rows){
               LinkParent(fle,kyln,pgsz);
            }else{
               for(i=row;i<Rows;i++){
                  Table[i].Key = Table[i + 1].Key;
                  Table[i].Address = Table[i + 1].Address;
               }
            }
            fle.Write(this);
            fle.Write(root);
            return;
         }

         ushort pz,nz,f,t,dx;
         IdxPage pv,nx;

         if(Previous>0){
            pv = new IdxPage(0,0,0,kyln);
            fle.Read(pv,Previous,pgsz);
            pz = pv.Rows;
         }else{
            pv = null;
            pz = coOrder+coOrder;
         }
         if(Next>0){
            nx = new IdxPage(0,0,0,kyln);
            fle.Read(nx,Next,pgsz);
            nz = nx.Rows;
         }else{
            nx = null;
            nz = coOrder+coOrder;
         }

         Rows--;
         if(pz<nz){
            if(pv.Rows==coMin){
               t = coMin;
               f = 0;
               for(i=0;i<Rows;i++){
                  if(i==row) f++;
                  pv.Table[t].Key     = Table[f].Key;
                  pv.Table[t].Address = Table[f].Address;
                  if(branch) pv.LinkChild(fle,Table[f].Address,kyln,pgsz);
                  t++;
                  f++;
               }
               pv.Rows += Rows;
               pv.Next  = Next;
               if(Next==0){
                  if(!branch) root.Last = Previous;
                  if(pv.Previous==0){
                     pv.Parent = 0;
                     fle.Write(pv);
                     root.Root = Previous;
                     root.Levels--;
                     fle.Write(root);
                     return;
                  }
               }else{
                  nx.Previous  = Previous;
                  fle.Write(nx);
               }
               pv.LinkParent(fle,kyln,pgsz);
               fle.Write(pv);
               KeyOnParent(fle,kyln,pgsz,pos);
               pos.Page.Remove(fle,Table[Rows].Key,this.Buffer.Position,true,root,pos);
               return;
            }

            dx = (ushort)((pv.Rows-coMin)/2);
            if(dx==0) dx = 1;
            t = (ushort)(Rows+dx-1);
            f = Rows;
            for(i=0;i<Rows;i++){
               if(f==row) f--;
               Table[t].Key     = Table[f].Key;
               Table[t].Address = Table[f].Address;
               f--;
               t--;
            }
            f = (ushort)(pv.Rows-1);
            for(i=0;i<dx;i++){
               Table[t].Key     = pv.Table[f].Key;
               Table[t].Address = pv.Table[f].Address;
               if(branch) LinkChild(fle,Table[t].Address,kyln,pgsz);
               t--;
               f--;
            }
            pv.Rows -= dx;
            Rows += dx;
            if(row+dx==Rows) LinkParent(fle,kyln,pgsz);
            pv.LinkParent(fle,kyln,pgsz);
            fle.Write(pv);
            fle.Write(this);
            fle.Write(root);
            return;
         }

         if(nx.Rows==coMin){
            f = (ushort)(nx.Rows-1);
            t = (ushort)(f+Rows);
            for(i=0;i<nx.Rows;i++){
               nx.Table[t].Key     = nx.Table[f].Key;
               nx.Table[t].Address = nx.Table[f].Address;
               t--;
               f--;
            }
            f = Rows;
            for(i=0;i<Rows;i++){
               if(f==row) f--;
               nx.Table[t].Key     = Table[f].Key;
               nx.Table[t].Address = Table[f].Address;
               if(branch) nx.LinkChild(fle,Table[f].Address,kyln,pgsz);
               t--;
               f--;
            }
            nx.Rows += Rows;
            nx.Previous = Previous;
            if(Previous==0){
               if(!branch) root.First = Next;
               if(nx.Next==0){
                  nx.Parent = 0;
                  fle.Write(nx);
                  root.Root = Next;
                  root.Levels--;
                  fle.Write(root);
                  return;
               }
            }else{
               pv.Next = Next;
               fle.Write(pv);
            }
            fle.Write(nx);
            KeyOnParent(fle,kyln,pgsz,pos);
            pos.Page.Remove(fle,Table[Rows].Key,this.Buffer.Position,true,root,pos);
            return;
         }

         for(i=row;i<Rows;i++){
            Table[i].Key     = Table[i+1].Key;
            Table[i].Address = Table[i+1].Address;
         }

         dx = (ushort)((nx.Rows-coMin)/2);
         if(dx==0) dx = 1;

         t = Rows;
         f = 0;
         for(i=0;i<dx;i++){
            Table[t].Key     = nx.Table[f].Key;
            Table[t].Address = nx.Table[f].Address;
            if(branch) LinkChild(fle,Table[t].Address,kyln,pgsz);
            f++;
            t++;
         }
         Rows += dx;
         nx.Rows -= dx;
         t = 0;
         for(i=0;i<nx.Rows;i++){
            nx.Table[t].Key     = nx.Table[f].Key;
            nx.Table[t].Address = nx.Table[f].Address;
            t++;
            f++;
         }
         LinkParent(fle,kyln,pgsz);
         fle.Write(this);
         fle.Write(nx);
         fle.Write(root);

      }

      public void Check(DataFile fle,IdxRoot root,ushort lvl){
         int pgsz  = root.PageSize;
         ushort kyln = root.KeyLength;
         long fa,la,pa,na,xa,va;
         int i,j,c,ct;
         IdxPage fr,ls,mp,mn,pp;
         string ok;
         bool err,cr;

         if(Parent!=0) throw new System.InvalidOperationException("Wrong root page.");
         if(root.Root!=Buffer.Position) throw new System.InvalidOperationException("Wrong root page.");

         fr = new IdxPage(0,0,0,kyln);
         ls = new IdxPage(0,0,0,kyln);
         pp = new IdxPage(0,0,0,kyln);

         cr = true;
         ct = 0;
         fa = root.First;
         la = root.Last;
         while(true){
            fle.Read(fr,fa,pgsz);
            if(fr.Previous!=0) throw new System.InvalidOperationException("Wrong first page.");

            fle.Read(ls,la,pgsz);
            if(ls.Next!=0) throw new System.InvalidOperationException("Wrong last page.");

            mp = fr;
            mn = ls;
            while(true){

               if(mp.Rows>coOrder || (mp.Rows<coMin && mp.Parent!=0)) throw new System.InvalidOperationException("Wrong number of rows.");

               if(mp.Parent!=0){
                  fle.Read(pp,mp.Parent,pgsz);
                  err = true;
                  for(j=0;j<pp.Rows;j++){
                     if(pp.Table[j].Address==mp.Buffer.Position){
                        if(pp.Table[j].Key!=mp.Table[mp.Rows-1].Key) throw new System.InvalidOperationException("Last key on this page does not match link key on parent page.");
                        err = false;
                        break;
                     }
                  }
                  if(err) throw new System.InvalidOperationException("Link with parent page is wrong.");
               }

               if(cr) ct += mp.Rows;

               ok = "";
               for(i=0;i<mp.Rows;i++){
                  c = string.Compare(ok,mp.Table[i].Key);
                  if(c>0){
                     throw new System.InvalidOperationException("Wrong key order.");
                  }
                  ok = mp.Table[i].Key;
               }

               if(mp.Next==0){
                  if(mn.Previous!=0) throw new System.InvalidOperationException("Wrong sibling link.");
                  break;
               }
               pa = mp.Buffer.Position;
               na = mn.Buffer.Position;
               xa = mp.Next;
               va = mn.Previous;
               mp = new IdxPage(0,0,0,kyln);
               mn = new IdxPage(0,0,0,kyln);
               fle.Read(mp,xa,pgsz);
               fle.Read(mn,va,pgsz);
               if(mp.Previous!=pa) throw new System.InvalidOperationException("Wrong next page.");
               if(mn.Next!=na) throw new System.InvalidOperationException("Wrong previous page.");

               c = string.Compare(ok,mp.Table[0].Key);
               if(c>0) throw new System.InvalidOperationException("Wrong page-to-page key order.");
            }
            if(cr){
               cr = false;
               if(ct!=root.Keys) throw new System.InvalidOperationException("Wrong parent link.");
            }
            fa = fr.Parent;
            la = ls.Parent;
            if(fa==0 || la==0){
               if(la!=0 || fa!=0) throw new System.InvalidOperationException("Wrong parent link.");
               break;
            }
         }

      }

   }

   internal class IdxRoot : Record{
      internal long Root;
      internal long First;
      internal long Last;
      internal long Keys;
      internal ushort Levels;
      internal ushort KeyLength;
      internal int PageSize;
      internal DateTime LastUpdate;
      internal bool Closed;
      public void GetFirst(DataFile fle,IdxPos pos){
         if(Root==0){
            pos.PageAdr = 0;
            pos.RowAdr  = 0;
            pos.Row     = 0;
            pos.Result  = IdxPos.coIxBigger;
            return;
         }
         pos.Page = new IdxPage(0,0,0,KeyLength);
         fle.Read(pos.Page,First,PageSize);
         pos.Page.First(pos);
      }
      public void GetLast(DataFile fle,IdxPos pos){
         if(Root==0){
            pos.PageAdr = 0;
            pos.RowAdr  = 0;
            pos.Row     = 0;
            pos.Result  = IdxPos.coIxBigger;
            return;
         }
         pos.Page = new IdxPage(0,0,0,KeyLength);
         fle.Read(pos.Page,Last,PageSize);
         pos.Page.Last(pos);
      }
      public void Scan(DataFile fle,string key,IdxPos pos){
         if(Root==0){
            pos.PageAdr = 0;
            pos.RowAdr  = 0;
            pos.Row     = 0;
            pos.Result  = IdxPos.coIxBigger;
            return;
         }
         pos.Page    = new IdxPage(0,0,0,KeyLength);
         pos.PageAdr = Root;
         ushort l    = Levels;
         while(true){
            fle.Read(pos.Page,pos.PageAdr,PageSize);
            pos.Page.Scan(key,pos);
            l--;
            if(l==0) return;
            pos.PageAdr = pos.RowAdr;
         }
      }
      public bool Match(DataFile fle,string key,long adr,IdxPos pos){
         while(true){
            pos.Page.Match(key,adr,pos);
            if(pos.Result==IdxPos.coIxMatch) return true;
            if(pos.Result!=IdxPos.coIxFound || pos.PageAdr==0) return false;
            fle.Read(pos.Page,pos.PageAdr,PageSize);
         }
      }
      public bool Jump(DataFile fle,long jmp,IdxPos pos){
         pos.OnPage = true;
         while(true){
            jmp = pos.Page.Jump(jmp,pos);
            if(jmp==0) return true;
            if(pos.PageAdr==0) return false;
            fle.Read(pos.Page,pos.PageAdr,PageSize);
         }
      }

      public long Position(DataFile fle,IdxPos pos){
         pos.OnPage = true;
         long sk = 0;
         while(true){
            sk += pos.Page.Position(pos);
            if(pos.PageAdr==0) return sk;
            fle.Read(pos.Page,pos.PageAdr,PageSize);
         }
      }

      public void Add(DataFile fle,string key,long adr,IdxPos pos){
         LastUpdate = DateTime.Now;
         Keys++;
         if(Root==0){
            IdxPage pg  = new IdxPage(0,0,0,KeyLength);
            pos.Row    = 0;
            pos.Result = IdxPos.coIxBigger;
            pg.Add(fle,key,adr,false,this,pos);
            return;
         }
         pos.Page    = new IdxPage(0,0,0,KeyLength);
         pos.PageAdr = Root;
         ushort l = Levels;
         while(true){
            fle.Read(pos.Page,pos.PageAdr,PageSize);
            pos.Page.Scan(key,pos);
            l--;
            if(l==0) break;
            pos.PageAdr = pos.RowAdr;
         }
         pos.Page.Add(fle,key,adr,false,this,pos);
      }

      public void Remove(DataFile fle,string key,long adr,IdxPos pos){
         if(Root==0) throw new System.InvalidOperationException("MinBase: Index file "+fle.LoFile.Name+" corrupted: tying to delete key on empty index.");
         LastUpdate = DateTime.Now;
         Keys--;
         pos.Page    = new IdxPage(0,0,0,KeyLength);
         pos.PageAdr = Root;
         ushort l = Levels;
         while(true){
            fle.Read(pos.Page,pos.PageAdr,PageSize);
            pos.Page.Scan(key,pos);
            l--;
            if(l==0) break;
            pos.PageAdr = pos.RowAdr;
         }
         if(pos.Result==IdxPos.coIxFound){
            if(Match(fle,key,adr,pos)){
               pos.Page.Remove(fle,key,adr,false,this,pos);
               return;
            }
         }
         throw new System.InvalidOperationException("MinBase: Index file "+fle.LoFile.Name+" corrupted: tying to delete unexistent key.");
      }

      public void Check(DataFile fle){
         if(Root==0){
            if(Keys>0) throw new System.InvalidOperationException("Index has no root but does contain keys.");
            return;
         }
         if(Keys==0) throw new System.InvalidOperationException("Index is not empty while it should.");
         IdxPage pg = new IdxPage(0,0,0,KeyLength);
         fle.Read(pg,Root,PageSize);
         pg.Check(fle,this,Levels);
      }

      public override void OnLoad(BinaryReader rd){
         Root       = rd.ReadInt64();
         First      = rd.ReadInt64();
         Last       = rd.ReadInt64();
         Keys       = rd.ReadInt64();
         Levels     = rd.ReadUInt16();
         KeyLength  = rd.ReadUInt16();
         PageSize   = rd.ReadInt32();
         LastUpdate = DateTime.FromBinary(rd.ReadInt64());
         Closed     = rd.ReadBoolean();
      }
      public override void OnSave(BinaryWriter wr){
         wr.Write(Root);
         wr.Write(First);
         wr.Write(Last);
         wr.Write(Keys);
         wr.Write(Levels);
         wr.Write(KeyLength);
         wr.Write(PageSize);
         wr.Write(LastUpdate.ToBinary());
         wr.Write(Closed);
      }
   }

   internal class Index{

      internal DataFile File;
      internal IdxRoot Root = new IdxRoot();

      public Index(string nm,ushort kl,DateTime udTime){
         int sz = DataFile.GetSize(Root);
         if(DataFile.Exists(nm,"Index")){
            File = DataFile.Open(nm,"Index",FileMode.Open);
            File.Read(Root,DataFile.coSignatureSz,sz);
            if(DateTime.Compare(Root.LastUpdate,udTime)>=0 && Root.Closed){
               Root.Closed = false;
               File.Lock(Root);
               File.Write(Root);
               return;
            }
            File.Close();
            DataFile.Delete(nm,"Index");
         }
         File = DataFile.Open(nm,"Index",FileMode.CreateNew);
         DataFile.Blank(Root,sz);
         Root.KeyLength = kl;
         Root.Closed    = false;
         Root.PageSize  = (ushort)DataFile.GetSize(new IdxPage(0,0,0,kl));
         File.Append(Root,sz);
         File.Lock(Root);
      }
      public void Close(){
         Root.Closed = true;
         File.Write(Root);
         File.Close();
      }
      public long Keys(){
         return Root.Keys;
      }
      public long First(){
         IdxPos pos = new IdxPos();
         File.BeginUpdate();
         Root.GetFirst(File,pos);
         File.EndUpdate();
         return pos.RowAdr;
      }
      public long Last(){
         IdxPos pos = new IdxPos();
         File.BeginUpdate();
         Root.GetLast(File,pos);
         File.EndUpdate();
         return pos.RowAdr;
      }
      public long Seek(string key,bool soft){
         IdxPos pos = new IdxPos();
         File.BeginUpdate();
         Root.Scan(File,key,pos);
         File.EndUpdate();
         if(soft){
            if(pos.Result==IdxPos.coIxSmaller) return 0;
            return pos.RowAdr;
         }else{
            if(pos.Result==IdxPos.coIxFound) return pos.RowAdr;
            return 0;
         }
      }
      public bool Sync(string key,long row){
         bool rv;
         IdxPos pos = new IdxPos();
         File.BeginUpdate();
         Root.Scan(File,key,pos);
         if(pos.Result==IdxPos.coIxFound){
            rv = Root.Match(File,key,row,pos);
         }else{
            rv = false;
         }
         File.EndUpdate();
         return rv;
      }
      public long Jump(string key,long row,long sk){
         IdxPos pos = new IdxPos();
         File.BeginUpdate();
         Root.Scan(File,key,pos);
         if(pos.Result==IdxPos.coIxFound){
            if(Root.Match(File,key,row,pos)){
               if(Root.Jump(File,sk,pos)){
                  File.EndUpdate();
                  return pos.RowAdr;
               }
            }
         }
         File.EndUpdate();
         return 0;
      }
      public void Add(string key,long row){
         if(key.Length!=Root.KeyLength) throw new System.InvalidOperationException("MinBase: attempt to add a key with an invalid length for this index.");
         IdxPos pos = new IdxPos();
         File.BeginUpdate();
         Root.Add(File,key,row,pos);
         File.EndUpdate();
      }
      public void Remove(string key,long row){
         IdxPos pos = new IdxPos();
         File.BeginUpdate();
         Root.Remove(File,key,row,pos);
         File.EndUpdate();
      }
      public long Position(string key,long row){
         IdxPos pos = new IdxPos();
         File.BeginUpdate();
         Root.Scan(File,key,pos);
         if(pos.Result==IdxPos.coIxFound){
            if(Root.Match(File,key,row,pos)){
               long sk = Root.Position(File,pos);
               File.EndUpdate();
               return sk;
            }
         }
         File.EndUpdate();
         return 0;
      }
      public void Check(){
         Root.Check(File);
      }

   }

   internal class View{
      private Index index;
      internal mpIndexKey getkey;
      private ushort count;
      internal bool delete;
      public View(){
      }
      public bool MustBuild(string nm,DateTime udTime,mpIndexKey gk,Table obj){
         string tk = gk(obj);
         index = new Index(nm,(ushort)tk.Length,udTime);
         getkey = gk;
         count = 1;
         return index.Keys()==0;
      }
      public void Grow(){
         count++;
      }
      public bool Close(){
         if(count==1){
            index.Close();
            if(delete) File.Delete(index.File.LoFile.Name);
            return true;
         }
         count--;
         return false;
      }
      public void DoClose(){
         index.Close();
         if(delete) File.Delete(index.File.LoFile.Name);
      }
      public void StatusCheck(Table orow,Table nrow,long row){
         string okey = getkey(orow);
         string nkey = getkey(nrow);
         if(okey==""){
            if(nkey=="") return;
            index.Add(nkey,row);
         }else{
            if(okey==nkey) return;
            index.Remove(okey,row);
            if(nkey!="") index.Add(nkey,row);
         }
      }
      public void AddKey(Table obj,long row){
         string ky = getkey(obj);
         if(ky!="") index.Add(ky,row);
      }
      public bool Sync(Table obj,long row){
         string ky = getkey(obj);
         return index.Sync(ky,row);
      }
      public long First(){
         return index.First();
      }
      public long Last(){
         return index.Last();
      }
      public long Seek(string key,bool soft){
         return index.Seek(key,soft);
      }
      public long Jump(Table obj,long row,long sk){
         string ky = getkey(obj);
         return index.Jump(ky,row,sk);
      }
      public long Keys(){
         return index.Keys();
      }
      public long Position(Table obj,long row){
         string ky = getkey(obj);
         return index.Position(ky,row);
      }
      public void Check(){
         index.Check();
      }

      public void CheckRow(Table obj,long row){
         string key = getkey(obj);
         if(key=="") return;
         if(!index.Sync(key,row)){
            throw new System.AccessViolationException("MinBase: Key/Address pair out of sync.");
         }
      }
   }

}

// End of file.