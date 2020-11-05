// 
// MinBase
// Copyright (c) Casablanca Sistemas S.A. de C.V.
// Jorge Reyna Tamez.
// 2014-02-12
//

using System;
using System.Collections.Generic;
using System.IO;
using System.Text;

namespace gBase{

   internal delegate void mpExtraLoad(BinaryReader rd);
   internal delegate void mpExtraSave(BinaryWriter wr);

   abstract public class Record {
      internal Buffer Buffer;
      public abstract void OnLoad(BinaryReader rd);
      public abstract void OnSave(BinaryWriter wr);
      internal mpExtraLoad OnLoadExtra;
      internal mpExtraSave OnSaveExtra;
      public static string Fix(string str,int size) {
         if(str==null) return "".PadRight(size);
         if(str.Length>size) return str.Substring(0,size);
         return str.PadRight(size);
      }
      public static void WriteFix(BinaryWriter wr,string str,int size) {
         wr.Write(Encoding.Unicode.GetBytes(Fix(str,size)));
      }
      public static string ReadFix(BinaryReader rd,int size){
         return Encoding.Unicode.GetString(rd.ReadBytes(size*2));
      }
   }

   internal class Buffer : IDisposable {
      public DataFile File;
      public MemoryStream Mem;
      public BinaryReader Reader;
      public BinaryWriter Writer;
      public long Used;
      public long Position;
      public int Size;
      public uint Locks;

      public void Dispose() {
         Dispose(true);
         GC.SuppressFinalize(this);
      }

      protected virtual void Dispose(bool disposing) {
         if(disposing) {
            if(Mem!=null) {
               Mem.Dispose();
               Mem = null;
            }
            if(Reader!=null) {
               Reader.Close();
               Reader = null;
            }
            if(Writer!=null) {
               Writer.Close();
               Writer = null;
            }
         }
      }
   }

   internal class DataFile : IDisposable {

      internal const long coSignatureSz = sizeof(long);
      private const long coSignature   = 0x6B696C626F47;
      private const int coMaxBuffer   = 64;

      public static string dbPath = NoFile(System.IO.Path.GetDirectoryName(System.Reflection.Assembly.GetExecutingAssembly().GetName().CodeBase));

      internal FileStream LoFile;

      private bool disposed = false;
      private long Updates = 0;
      private long BufUse  = 0;
      private System.Threading.Mutex SyncBuf = new System.Threading.Mutex();
      private Dictionary<long,Buffer> buffer = new Dictionary<long,Buffer>();

      private static string NoFile(string s) {
         if(s.StartsWith("file:\\")) return s.Substring(6,s.Length-6);
         if(s.StartsWith("file:/")) return s.Substring(5,s.Length-5);
         return s;
      }
      private DataFile() {
      }

      public void Dispose() {
         Dispose(true);
         GC.SuppressFinalize(this);
      }

      protected virtual void Dispose(bool disposing) {
         if(disposed) return;
         if(disposing) {
            if(LoFile!=null) {
               LoFile.Dispose();
               LoFile = null;
            }
            buffer = null;
         }
         disposed = true;
      }

      public static int GetSize(Record obj) {
         MemoryStream m = new MemoryStream(4096);
         BinaryWriter w = new BinaryWriter(m);
         obj.OnSave(w);
         if(obj.OnSaveExtra!=null) obj.OnSaveExtra(w);
         int sz = (int) m.Position;
         w.Close();
         m.Close();
         return sz;
      }

      public static void Blank(Record obj,int sz) {
         obj.Buffer = null;
         byte[] b = new byte[sz];
         MemoryStream m = new MemoryStream(sz);
         BinaryReader r = new BinaryReader(m);
         BinaryWriter w = new BinaryWriter(m);
         w.Write(b,0,sz);
         m.Position = 0;
         obj.OnLoad(r);
         if(obj.OnLoadExtra!=null) obj.OnLoadExtra(r);
         w.Close();
         r.Close();
         m.Close();
      }

      public static bool Exists(string name,string type) {
         return File.Exists(dbPath+"/"+name+"."+type);
      }

      public static void Delete(string name,string type) {
         File.Delete(dbPath+"/"+name+"."+type);
      }

      public static DataFile Open(string name,string type,FileMode mo) {
         DataFile fle    = new DataFile();
         string fname  = dbPath+"/"+name+"."+type;
         bool exists = File.Exists(fname);
         switch(mo) {
         case FileMode.CreateNew:
            if(exists) throw new System.InvalidOperationException("MinBase: Cannot create file "+fname+" because it already exists.");
            if(!Directory.Exists(dbPath)) Directory.CreateDirectory(dbPath);
            break;
         case FileMode.Open:
            if(!exists) throw new System.InvalidOperationException("MinBase: Cannot open file "+fname+" becuase it does not exist.");
            break;
         }
         fle.LoFile = new FileStream(fname,mo);
         if(mo==FileMode.CreateNew) {
            byte[] si = BitConverter.GetBytes(coSignature);
            fle.LoFile.Write(si,0,sizeof(long));
         } else {
            byte[] si = new byte[sizeof(long)];
            fle.LoFile.Read(si,0,sizeof(long));
            if(BitConverter.ToInt64(si,0)!=coSignature) throw new System.InvalidOperationException("MinBase: File "+fle.LoFile.Name+" is not in a valid MinBase format.");
         }
         return fle;
      }

      public void Close() {
         LoFile.Close();
      }


      public void BeginUpdate() {
         SyncBuf.WaitOne();
         if(Updates==0 && buffer.Count>coMaxBuffer) {
            int c=0;
            int d = buffer.Count-coMaxBuffer;
            int i;
            long[] l = new long[d];
            long o = BufUse-coMaxBuffer;
            Buffer m;
            foreach(KeyValuePair<long,Buffer> b in buffer) {
               if(b.Value.Used<o && b.Value.Locks==0 && c<d) l[c++] = b.Key;
            }
            for(i=0;i<c;i++) {
               if(buffer.TryGetValue(l[i],out m)) {
                  buffer.Remove(l[i]);
                  m.Mem.Close();
               }
            }
         }
         Updates++;
         SyncBuf.ReleaseMutex();
      }

      public void EndUpdate() {
         SyncBuf.WaitOne();
         if(Updates>0) Updates--;
         SyncBuf.ReleaseMutex();
      }

      public void Read(Record obj,long ofs,int sz) {
         Buffer buf;
         SyncBuf.WaitOne();
         if(buffer.TryGetValue(ofs,out buf)) {
            buf.Used = BufUse++;
            buf.Mem.Position = 0;
            obj.OnLoad(buf.Reader);
            if(obj.OnLoadExtra!=null) obj.OnLoadExtra(buf.Reader);
            obj.Buffer = buf;
            SyncBuf.ReleaseMutex();
            return;
         }
         buf          = new Buffer();
         buf.File     = this;
         buf.Mem      = new MemoryStream(sz);
         buf.Reader   = new BinaryReader(buf.Mem);
         buf.Writer   = new BinaryWriter(buf.Mem);
         buf.Position = LoFile.Position = ofs;
         buf.Size     = sz;
         obj.Buffer   = buf;
         buf.Used     = BufUse++;
         buffer.Add(ofs,buf);
         SyncBuf.ReleaseMutex();
         byte[] tmp   = new byte[sz];
         LoFile.Read(tmp,0,sz);
         buf.Mem.Write(tmp,0,sz);
         buf.Mem.Position = 0;
         obj.OnLoad(buf.Reader);
         if(obj.OnLoadExtra!=null) obj.OnLoadExtra(buf.Reader);
      }

      public void Append(Record obj,int sz) {
         Buffer buf = new Buffer();
         buf.File     = this;
         buf.Mem      = new MemoryStream(sz);
         buf.Reader   = new BinaryReader(buf.Mem);
         buf.Writer   = new BinaryWriter(buf.Mem);
         buf.Position = LoFile.Position = LoFile.Length;
         buf.Size     = sz;
         obj.Buffer   = buf;
         SyncBuf.WaitOne();
         buf.Used     = BufUse++;
         buffer.Add(LoFile.Length,buf);
         SyncBuf.ReleaseMutex();
         obj.OnSave(buf.Writer);
         if(obj.OnSaveExtra!=null) obj.OnSaveExtra(buf.Writer);
         if(buf.Mem.Position!=sz) throw new System.InvalidOperationException("MinBase: Unable to write object to file "+LoFile.Name+" due to an object size missmatch. Expected size: "+sz.ToString()+". Actual size: "+buf.Mem.Position.ToString()+".");
         byte[] tmp = buf.Mem.ToArray();
         LoFile.Write(tmp,0,sz);
      }

      public void Write(Record obj) {
         Buffer buf = obj.Buffer;
         if(buf==null) throw new System.InvalidOperationException("MinBase: Unable to overwrite an object which wasn't read or appended first at file "+LoFile.Name+".");
         SyncBuf.WaitOne();
         if(!buffer.TryGetValue(buf.Position,out buf)) buffer.Add(buf.Position,buf);
         buf.Used         = BufUse++;
         SyncBuf.ReleaseMutex();
         buf.Mem.Position = 0;
         obj.OnSave(buf.Writer);
         if(obj.OnSaveExtra!=null) obj.OnSaveExtra(buf.Writer);
         if(buf.Mem.Position!=buf.Size) throw new System.InvalidOperationException("MinBase: Unable to write object to file "+LoFile.Name+" due to an object size missmatch.");
         LoFile.Position = buf.Position;
         byte[] tmp = buf.Mem.ToArray();
         LoFile.Write(tmp,0,buf.Size);
      }

      public void Lock(Record obj) {
         obj.Buffer.Locks++;
      }

      public void Unlock(Record obj) {
         if(obj.Buffer.Locks>0) obj.Buffer.Locks--;
      }

      public void CopyTo(string name,string type) {
         FileStream fle = new FileStream(dbPath+"/"+name+"."+type,FileMode.Create);
         long lf = LoFile.Length;
         long bz = 1000000;
         long tor = (lf<bz ? lf : bz);
         byte[] tmp = new byte[tor];
         while(lf>0) {
            LoFile.Read(tmp,0,(int) tor);
            fle.Write(tmp,0,(int) tor);
            if(lf>tor) {
               lf -= tor;
               if(lf<tor) tor = lf;
            } else {
               break;
            }
         }
         fle.Close();
      }

   }
}

// End of file.