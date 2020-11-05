// 
// MinBase
// Copyright (c) Casablanca Sistemas S.A. de C.V.
// Jorge Reyna Tamez.
// 2014-02-12
//

using System;
using System.Collections.Generic;
using System.IO;

namespace gBase{

   internal class Struct : Record{
      internal int RowLength;
      internal long RowCount;
      internal DateTime LastUpdate;
      public override void OnLoad(BinaryReader rd){
         RowLength  = rd.ReadInt32();
         RowCount   = rd.ReadInt64();
         LastUpdate = DateTime.FromBinary(rd.ReadInt64());
      }
      public override void OnSave(BinaryWriter wr){
         wr.Write(RowLength);
         wr.Write(RowCount);
         wr.Write(LastUpdate.ToBinary());
      }
   }

   abstract public class Table : Record{
      
      public string Name;

      internal long      RowNo;
      internal ushort    Flags;
      internal TableFile LoTable;
      internal View      Order;


      public long Row{
         get{return RowNo;}
         set{            
            if(value<1 || value>LoTable.Rows()){
               RowNo = 0;
               LoTable.Blank(this);
               return;
            }
            RowNo = value;
            LoTable.Read(this,value);
            if(Order!=null && !Order.Sync(this,value)){
               RowNo = 0;
               LoTable.Blank(this);
            }
         }
      }

      public Table(){
         OnLoadExtra = ExtraLoad;
         OnSaveExtra = ExtraSave;
      }
      internal void ExtraLoad(BinaryReader rd){
         Flags = rd.ReadUInt16();
      }
      internal void ExtraSave(BinaryWriter wr){
         wr.Write(Flags);
      }

      public long Rows(){
         if(Order==null) return LoTable.Rows();
         return Order.Keys();
      }

      public void First(){
         if(Order==null){
            Row = 1;
         }else{
            Row = Order.First();
         }
      }

      public void Last(){
         if(Order==null){
            Row = LoTable.Rows();
         }else{
            Row = Order.Last();
         }
      }

      public void Previous(){
         if(RowNo==0) return;
         if(Order==null){
            Row = RowNo-1;
         }else{
            Row = Order.Jump(this,RowNo,-1);
         }
      }

      public void Next(){
         if(RowNo==0) return;
         if(Order==null){
            Row = RowNo+1;
         }else{
            Row = Order.Jump(this,RowNo,1);
         }
      }

      public void Jump(long jmp){
         if(RowNo==0) return;
         if(Order==null){
            Row = RowNo+jmp;
         }else{
            Row = Order.Jump(this,RowNo,jmp);
         }
      }

      public long Position(){
         if(RowNo==0) return 0;
         if(Order==null) return RowNo;
         return Order.Position(this,RowNo);
      }

      public bool Find(string key,bool soft){
         if(Order==null) throw new System.InvalidOperationException("MinBase: Trying to access table " + LoTable.File.LoFile.Name + " as indexed with no index is active.");
         RowNo = Order.Seek(key,soft);
         if(RowNo==0){
            LoTable.Blank(this);
            return false;
         }
         LoTable.Read(this,RowNo);
         return true;
      }
      public bool Find(string key){
         return Find(key,false);
      }

      public void Add(){
         LoTable.Add(this);
         if(Order!=null && !Order.Sync(this,RowNo)){
            RowNo = 0;
            LoTable.Blank(this);
         }
      }

      public bool Lock(){
         return LoTable.Lock(this);
      }

      public void Unlock(){
         LoTable.Unlock(this);
      }

      public void WriteAndUnlock(){
         LoTable.WriteAndUnlock(this);
         if(Order!=null && !Order.Sync(this,RowNo)){
            RowNo = 0;
            LoTable.Blank(this);
         }
      }

      public void Delete(){
         LoTable.Delete(this);
      }

      public void Recall(){
         LoTable.Recall(this);
      }

      public bool Deleted(){
         return LoTable.Deleted(this);
      }

      public void Pack(){
         LoTable.Pack();
      }

      public static void SetPath(string path){
         DataFile.dbPath = path;
      }
      public static string GetPath(){
         return DataFile.dbPath;
      }

      public bool Exists(){
         return TableFile.Exists(Name);
      }

      public void Destroy(){         
         TableFile.Destroy(Name);
      }

      public void Create(){
         if(LoTable!=null) throw new System.InvalidOperationException("MinBase: Attempt to overwrite a table which is open.");
         TableFile.Create(Name,this);
      }

      public void Open(){
         if(LoTable!=null) Close();
         TableFile.Open(Name,this,null,null);
      }
      public void Open(mpIndexKey getKey){
         if(LoTable!=null) Close();
         TableFile.Open(Name,this,getKey,null);
      }
      public void Open(mpIndexKey getKey,string xName){
         if(LoTable!=null) Close();
         TableFile.Open(Name,this,getKey,xName);
      }

      public void Close(){
         LoTable.Close(this);
      }

      public void Check(){
         LoTable.Check(this);
      }

   }

   internal class RowLock{
      private int id;
      private ushort count;
      public RowLock(){
         id    = System.Threading.Thread.CurrentThread.ManagedThreadId;
         count = 1;
      }
      public bool Lock(){
         if(id==System.Threading.Thread.CurrentThread.ManagedThreadId){
            count++;
            return true;
         }
         return false;
      }
      public bool Own(){
         return (id==System.Threading.Thread.CurrentThread.ManagedThreadId);
      }
      public bool Unlock(){
         if(count==1) return true;
         count--;
         return false;
      }
   }

   internal sealed class TableFile{

      private const ushort coDeleted = 0x0001;

      private static System.Threading.Mutex SyncOpen = new System.Threading.Mutex();
      private static Dictionary<string,TableFile> OpenList = new Dictionary<string,TableFile>();

      private System.Threading.Mutex SyncView  = new System.Threading.Mutex();
      private System.Threading.Mutex SyncWrite = new System.Threading.Mutex();
      private Dictionary<long,RowLock> LockList  = new Dictionary<long,RowLock>();
      private Dictionary<mpIndexKey,View> ViewList  = new Dictionary<mpIndexKey,View>();
      internal string Alias;
      internal DataFile File;
      private Struct Struct;
      private ushort OpenCount;
      private long Data;
      private bool MustPack;

      private TableFile(){
      }

      internal static bool Exists(string nm){
         return DataFile.Exists(nm,"Table");
      }

      internal static void Destroy(String nm){
         TableFile db;
         if(OpenList.TryGetValue(nm,out db)) throw new System.InvalidOperationException("MinBase: Cannot delete table "+nm+" because it is currently open.");
         DataFile.Delete(nm,"Table");
      }

      internal static void Create(string nm,Table obj){
         TableFile db;
         if(OpenList.TryGetValue(nm,out db)) throw new System.InvalidOperationException("MinBase: Cannot create table "+nm+" because there is a perviously open table with the same name.");
         db = new TableFile();
         Struct stt = new Struct();
         db.File = DataFile.Open(nm,"Table",FileMode.CreateNew);
         stt.RowLength = DataFile.GetSize(obj);
         stt.RowCount  = 0;
         db.File.Append(stt,DataFile.GetSize(stt));
         db.File.Close();
      }

      internal static void Open(string nm,Table obj,mpIndexKey gkey,string xn){
         TableFile db;
         SyncOpen.WaitOne();
         if(OpenList.TryGetValue(nm,out db)){
            db.OpenCount++;
         }else{
            db = new TableFile();
            db.Alias = nm;
            db.File = DataFile.Open(nm,"Table",FileMode.Open);
            db.Struct = new Struct();
            int sz = DataFile.GetSize(db.Struct);
            db.File.Read(db.Struct,DataFile.coSignatureSz,sz);
            db.File.Lock(db.Struct);
            db.OpenCount = 1;
            db.Data = DataFile.coSignatureSz + sz;
            long cz = db.Data + db.Struct.RowLength * db.Struct.RowCount;
            long fz = db.File.LoFile.Length;
            if(cz!=fz){
               db.Struct.LastUpdate = DateTime.Now;
               db.File.Write(db.Struct);
               db.File.CopyTo(String.Format("{0}{1:yyyy}{1:MM}{1:dd}{1:HH}{1:mm}{1:ss}",nm,db.Struct.LastUpdate),"BackupTable");
               db.File.LoFile.SetLength(cz);
            }
            OpenList.Add(nm,db);
         }
         SyncOpen.ReleaseMutex();

         obj.LoTable = db;
         if(gkey==null){
            obj.Order = null;
         }else{
            obj.Order = db.SetView(obj,xn,gkey);
         }
         obj.First();
      }

      internal void Close(Table obj){
         SyncOpen.WaitOne();
         View vw = obj.Order;
         obj.LoTable = null;
         obj.Order = null;
         if(vw!=null && vw.Close()){
            SyncView.WaitOne();
            ViewList.Remove(vw.getkey);
            SyncView.ReleaseMutex();
         }
         if(OpenCount==1){
            string tmpfile = "";
            if(MustPack){
               Struct.LastUpdate = DateTime.Now;
               tmpfile = String.Format("{0}{1:yyyy}{1:MM}{1:dd}{1:HH}{1:mm}{1:ss}",Alias,Struct.LastUpdate);
               File.CopyTo(tmpfile,"BackupTable");
               long r,w,x;
               w = 1;
               x = Struct.RowCount;
               for(r = 1;r <= x;r++){
                  Read(obj,r);
                  if(!Deleted(obj)){
                     if(w!=r){
                        obj.Buffer.Position = Data + (w - 1) * Struct.RowLength;
                        File.Write(obj);
                     }
                     w++;
                  }
               }
               Struct.LastUpdate = DateTime.Now;
               Struct.RowCount   = w-1;
               File.LoFile.SetLength(Data+Struct.RowLength*Struct.RowCount);
            }
            File.Write(Struct);
            File.Close();
            if(MustPack) DataFile.Delete(tmpfile,"BackupTable");
            OpenList.Remove(Alias);
         }else{
            OpenCount--;

         }
         SyncOpen.ReleaseMutex();
      }

      internal View SetView(Table tmp,string nm,mpIndexKey rky){
         SyncView.WaitOne();
         View vw;
         if(ViewList.TryGetValue(rky,out vw)){
            vw.Grow();
            SyncView.ReleaseMutex();
            return vw;
         }
         vw = new View();
         if(nm==null || nm.Length==0){
            nm = Alias + ViewList.Count.ToString();
            vw.delete = true;
         }
         DataFile.Blank(tmp,Struct.RowLength);
         if(vw.MustBuild(nm,Struct.LastUpdate,rky,tmp)){
            long r,x;
            x = Struct.RowCount;
            for(r=1;r<=x;r++){
               Read(tmp,r);
               vw.AddKey(tmp,r);
            }
         }
         ViewList.Add(rky,vw);
         SyncView.ReleaseMutex();
         return vw;
      }

      internal long Rows(){
         return Struct.RowCount;
      }

      internal void Blank(Table obj){
         DataFile.Blank(obj,Struct.RowLength);
      }

      internal void Read(Table obj,long row){
         long rec = Data+(row-1)*Struct.RowLength;
         File.BeginUpdate();
         File.Read(obj,rec,Struct.RowLength);
         File.EndUpdate();
         obj.RowNo = row;
      }

      internal void Add(Table obj){
         SyncWrite.WaitOne();
         obj.Flags = 0;
         Struct.LastUpdate = DateTime.Now;
         Struct.RowCount++;
         obj.RowNo = Struct.RowCount;
         File.BeginUpdate();
         File.Write(Struct);
         File.Append(obj,Struct.RowLength);
         File.EndUpdate();
         File.LoFile.Flush();
         foreach(KeyValuePair<mpIndexKey,View> v in ViewList) v.Value.AddKey(obj,Struct.RowCount);
         SyncWrite.ReleaseMutex();
      }

      internal bool Lock(Table obj){
         if(obj.RowNo==0) throw new System.InvalidOperationException("MinBase: Attempt to lock an object which hasn't been loaded from file "+File.LoFile.Name+".");
         SyncWrite.WaitOne();
         RowLock lk;
         while(true){
            if(!LockList.TryGetValue(obj.RowNo,out lk)) break;
            bool rv =  lk.Lock();
            SyncWrite.ReleaseMutex();
            if(rv) return true;
            System.Threading.Thread.Sleep(0);
            SyncWrite.WaitOne();
         }
         lk = new RowLock();
         File.Lock(obj);
         LockList.Add(obj.RowNo,lk);
         Read(obj,obj.RowNo);
         SyncWrite.ReleaseMutex();
         return true;
      }

      internal void Unlock(Table obj){
         if(obj.RowNo>0){
            RowLock lk;
            SyncWrite.WaitOne();
            if(LockList.TryGetValue(obj.RowNo,out lk)){
               if(lk.Own()){
                  if(lk.Unlock()){
                     File.Unlock(obj);
                     LockList.Remove(obj.RowNo);
                     SyncWrite.ReleaseMutex();
                  }
                  return;
               }
            }
            SyncWrite.ReleaseMutex();
         }
         throw new System.InvalidOperationException("MinBase: Attempt to unlock an object which was not prevoiously locked at file "+File.LoFile.Name+".");
      }

      internal void WriteAndUnlock(Table obj){
         if(obj.RowNo>0){
            RowLock lk;
            SyncWrite.WaitOne();
            if(LockList.TryGetValue(obj.RowNo,out lk)){
               if(lk.Own()){
                  if(lk.Unlock()){
                     Table tmp   = (Table) Activator.CreateInstance(obj.GetType());
                     tmp.LoTable = obj.LoTable;
                     tmp.Order   = obj.Order;
                     File.BeginUpdate();
                     File.Read(tmp,obj.Buffer.Position,Struct.RowLength);
                     File.Write(obj);
                     Struct.LastUpdate = DateTime.Now;
                     File.Write(Struct);
                     File.EndUpdate();
                     File.LoFile.Flush();
                     foreach(KeyValuePair<mpIndexKey,View> v in ViewList) v.Value.StatusCheck(tmp,obj,obj.RowNo);
                     tmp.Order   = null;
                     tmp.LoTable = null; // keeps the file open.
                     File.Unlock(obj);
                     LockList.Remove(obj.RowNo);
                     SyncWrite.ReleaseMutex();
                  }
                  return;
               }
            }
            SyncWrite.ReleaseMutex();
         }
         throw new System.InvalidOperationException("MinBase: Attempt to unlock an object which was not prevoiously locked at file "+File.LoFile.Name+".");
      }

      internal void Delete(Table obj){
         RowLock lk;
         SyncWrite.WaitOne();
         if(LockList.TryGetValue(obj.RowNo,out lk)){
            obj.Flags |= coDeleted;
            SyncWrite.ReleaseMutex();
            return;
         }
         SyncWrite.ReleaseMutex();
      }

      internal void Recall(Table obj){
         RowLock lk;
         SyncWrite.WaitOne();
         if(LockList.TryGetValue(obj.RowNo,out lk)){
            obj.Flags = (ushort)(obj.Flags & ~coDeleted);
            SyncWrite.ReleaseMutex();
            return;
         }
         SyncWrite.ReleaseMutex();
      }

      internal bool Deleted(Table obj){
         return (obj.Flags & coDeleted)==coDeleted;
      }

      internal void Pack(){
         MustPack = true;
      }

      internal void Check(Table obj){
         SyncWrite.WaitOne();
         foreach(KeyValuePair<mpIndexKey,View> v in ViewList) v.Value.Check();
         long r,x;
         x = Struct.RowCount;
         for(r=1;r<=x;r++){
            Read(obj,r);
            foreach(KeyValuePair<mpIndexKey,View> v in ViewList) v.Value.CheckRow(obj,r);
         }
         SyncWrite.ReleaseMutex();
      }

   }

}

// End of file.