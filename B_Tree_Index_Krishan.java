package b._tree_index_krishan;
import java.io.*;
import java.util.*;
import java.nio.*;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * @author Krishan Kumar
 */
public class B_Tree_Index_Krishan {

    /**
     * @param args the command line arguments
     */
    static int order;                                      //order of b+ tree
    static boolean dup_entry;                              //Flag to indicate duplicate entry of the record.
    node parent;                                           // Parent/root node of the B+ tree
    public static RandomAccessFile raf_op;                 // Output index file
    public static RandomAccessFile raf_in;
    
    public B_Tree_Index_Krishan(String indx_file) throws IOException {
        raf_op = new RandomAccessFile(indx_file, "rw");    //Index file opened in rw mode
        //this.order = ord;
        parent = new leaf_nd();
    }
    
    abstract class node {
        ArrayList<String> key_list = new ArrayList<>();
        long cur_offset;                                   // current offset of each node in the index file  
        char leaf_flag;
        
        abstract void insert(String key, long offset);     // Function to insert key,offset in the node
        abstract node n_split();                           // Splitting this node & return the new created node
        abstract boolean is_full();                        // Check if this node is overflown
        abstract boolean is_leaf();                        // Check if this is leaf node
        abstract node read_fromFile(long offset);          // Read a node into memory at given offset from index file 
        abstract long write_toFile ();                     // Write a node into index file & return its offset 
    }
    
    //Leaf node
    class leaf_nd extends node {
        List<Long> d_ptr;
        long next;
        long read_offset;
        
        leaf_nd() {
            key_list = new ArrayList<>();
            d_ptr = new ArrayList<>();
            leaf_flag = 'L';
            cur_offset = -1;
        }
        
        @Override
        void insert(String key, long offset) {            
            //find position for the key to store in the key-list
            int pos_k = Collections.binarySearch(key_list, key);
            int pos_dptr;                                       // position of data-pointer
            long leftc_offset, rightc_offset;
            
            if(pos_k >= 0) {
                System.out.println("Key already exists in the tree");
                dup_entry = true;
            } else {
                pos_dptr = -(pos_k + 1);
                key_list.add(pos_dptr, key);
                d_ptr.add(pos_dptr, offset);
                //System.out.println("Added key- " + key + " to leafnode.");
            }
            //Split and create new nodes if the parent node is overflown
            if(parent.is_full()) {
                //System.out.println("Parent is full, splitting leaf node.");
                node temp = n_split();     
                leftc_offset = this.write_toFile();
                rightc_offset = temp.write_toFile();
                //Create an internal node now and make it new parent
                int_nd new_par = new int_nd();
                new_par.key_list.add(temp.key_list.get(0));     //add the 1st key from the temp-node               
                new_par.sub_ptr.add(leftc_offset);                      // Pointer to left subtree 
                new_par.sub_ptr.add(rightc_offset);                      // Pointer to right subtree,which is new created node
                parent = new_par;
                
                leftc_offset = new_par.write_toFile();              
                try {
                    update_parent(leftc_offset);
                    //System.out.println("New parent is created and added key-" + temp.key_list.get(0));
                } catch (IOException ex) {
                    Logger.getLogger(B_Tree_Index_Krishan.class.getName()).log(Level.SEVERE, null, ex);
                }
            }
        }
        
        // Function to write this node to the index file and return the offset where it's written
        @Override
        long write_toFile() {
            ByteBuffer buffer = ByteBuffer.allocate(1024);
            for(int i = 0; i < this.key_list.size(); i++){
                buffer.put(this.key_list.get(i).getBytes());
                buffer.putLong(this.d_ptr.get(i));
            }
            buffer.position(1016);
            buffer.putLong(this.next);
            buffer.position(1015);
            buffer.putChar('L');
            try {
                buffer.flip();
                if(this.cur_offset == -1) this.cur_offset = raf_op.length();
                raf_op.seek(this.cur_offset);
                raf_op.getChannel().write(buffer);
                raf_op.getChannel().position(1024);
                raf_op.writeBytes("\r\n");
            } catch (IOException ex) {
                Logger.getLogger(B_Tree_Index_Krishan.class.getName()).log(Level.SEVERE, null, ex);
            }  
            return this.cur_offset;
        }
        
        // Function to read a node from index file to memory 
        @Override
        node read_fromFile(long offset) {
            int key_len;
            char leaf_flag;
            byte[] keys; 
            leaf_nd lf = new leaf_nd();
            try {
                raf_op.seek(256);
                key_len = raf_op.readInt();
                
                raf_op.seek(offset);
                ByteBuffer buff = ByteBuffer.allocate(1024);
                buff.put(raf_op.readLine().getBytes());
                buff.position(1015);
                leaf_flag = buff.getChar();
                if(leaf_flag == 'L') {
                    for(int i = 0; i < 1015; i+=(key_len+8)) {
                        buff.position(i);
                        keys = new byte[key_len+1];
                        lf.key_list.add(buff.get(keys, i, key_len).toString());
                        lf.d_ptr.add(buff.getLong());
                    }
                    buff.position(1016);
                    lf.next = buff.getLong();
                    lf.cur_offset = offset;
                } else {
                    System.out.println("Not a leaf node!!");
                }
            } catch (IOException ex) {
                Logger.getLogger(B_Tree_Index_Krishan.class.getName()).log(Level.SEVERE, null, ex);
            }
            return lf;
        }
        
        @Override
        boolean is_full(){
            if(d_ptr.size() >= order) return true;
            else return false;
        }
        
        @Override
        //Function to split and create new node at same level 
        node n_split() {
            //temp node is created at same level
            long temp_offset;
            leaf_nd temp = new leaf_nd();
            int mid = key_list.size()/2;
            int end = key_list.size();
            
            //add the keys and data pointers whose index from mid till the end to the new node(temp)
            temp.key_list.addAll(key_list.subList(mid, end));
            temp.d_ptr.addAll(d_ptr.subList(mid, end));
            
            //remove the key & pointers from original node as they are added in temp node now!
            key_list.subList(mid, end).clear();
            
            //Now the new temp node should be pointed by the original node
            temp.next = this.next;
            temp_offset = temp.write_toFile();
            this.next = temp_offset;
            return temp;
        }
        
        @Override
        boolean is_leaf() {
            return true;
        }
    }
    
    //Internal node
    class int_nd extends node{
        List<Long> sub_ptr;   //pointer to subtree, i.e. child nodes
        long read_offset;     //offset in index file from where this internal node will be read
        
        int_nd() {
            key_list = new ArrayList<>();
            sub_ptr = new ArrayList<>();
            leaf_flag = 'I';
        }
        
        @Override
        void insert(String key, long offset) {
            //find position for the key to store in the key-list
            int pos_k = Collections.binarySearch(key_list, key);
            int pos_subptr;                                         // position of sub-tree pointer
            long sub_offset, leftc_offset, rightc_offset;
            node sub, curr;
            if(pos_k >= 0) {
                System.out.println("Key already exists in the tree");
                dup_entry = true;
            } else {
                pos_subptr = -(pos_k + 1);
                sub_offset = sub_ptr.get(pos_subptr);               // getting pointer to the sub-tree below
                sub = read_fromFile(sub_offset);                    // read sub/child into memory
                //System.out.println("Inserting the key-" + key + "in subtree/child.");
                
                sub.insert(key, offset);                             // inserting the key, offset in the sub-tree  
                
                //check if the sub-tree/child node is overflown.If yes, then one entry will be inserted in this node
                if(sub.is_full()) {
                    //System.out.println("subtree/child is full, splitting it. Called from internal node");
                    node temp = sub.n_split();     
                    leftc_offset = sub.write_toFile();               // write subtree node to file
                    rightc_offset = temp.write_toFile();             // write newcreated subtree node to file
                                
                    insert_sub(temp.key_list.get(0), rightc_offset); // insert middle value from children to current node
                    if(!temp.is_leaf()) {
                        temp.key_list.subList(0,1).clear();
                    }
                    //write current node to file as it has one new entry
                    this.write_toFile();                    
                }
                //If parent node is overflown, then split this internal node & create a new parent node
                if(parent.is_full()) {
                    //System.out.println("Parent is full, splitting it. Called from internal node");
                    node temp = n_split();
                    leftc_offset = parent.write_toFile();
                    rightc_offset = temp.write_toFile();
                    
                    int_nd new_par = new int_nd();
                    new_par.key_list.add(temp.key_list.get(0));
                    temp.key_list.subList(0,1).clear();
                    new_par.sub_ptr.add(leftc_offset);
                    new_par.sub_ptr.add(rightc_offset);   
                    leftc_offset = new_par.write_toFile();
                    try {
                        update_parent(leftc_offset);
                        //System.out.println("new parent created and added key-" + temp.key_list.get(0));
                    } catch (IOException ex) {
                        Logger.getLogger(B_Tree_Index_Krishan.class.getName()).log(Level.SEVERE, null, ex);
                    }
                }
            }
        }
        
        // Function to write internal node to index file
        @Override
        long write_toFile() {
            ByteBuffer buffer = ByteBuffer.allocate(1024);
            for(int i = 0; i < this.key_list.size(); i++){
                buffer.putLong(this.sub_ptr.get(i));
                buffer.put(this.key_list.get(i).getBytes());
            }
            buffer.putLong(this.sub_ptr.get(this.sub_ptr.size() - 1));
            buffer.position(1023);
            buffer.putChar('I');                          //put the flag that its internal node
            try {
                buffer.flip();
                if(this.cur_offset == -1) this.cur_offset = raf_op.length();
                raf_op.seek(this.cur_offset);
                raf_op.getChannel().write(buffer);
                raf_op.getChannel().position(1024);
                raf_op.writeBytes("\r\n");
            } catch (IOException ex) {
                Logger.getLogger(B_Tree_Index_Krishan.class.getName()).log(Level.SEVERE, null, ex);
            }  
            return this.cur_offset;
        }
        
        //Function to read internal node into memory
        @Override
        node read_fromFile(long offset) {
            int key_len;
            char intnd_flag;                     //Flag if its internal node
            byte[] keys; 
            int_nd in = new int_nd();
            try {
                raf_op.seek(256);
                key_len = raf_op.readInt();
                
                raf_op.seek(offset);
                ByteBuffer buff = ByteBuffer.allocate(1024);
                buff.put(raf_op.readLine().getBytes());
                buff.position(1023);
                intnd_flag = buff.getChar();
                if(intnd_flag == 'I') {
                    for(int i = 0; i < 1023; i+=(key_len+8)) {
                        buff.position(i);
                        in.sub_ptr.add(buff.getLong());
                        keys = new byte[key_len+1];
                        in.key_list.add(buff.get(keys, i, key_len).toString());                        
                    }
                    in.sub_ptr.add(buff.getLong());
                    in.cur_offset = offset;
                } else {
                    System.out.println("Not an internal node!!");
                }
            } catch (IOException ex) {
                Logger.getLogger(B_Tree_Index_Krishan.class.getName()).log(Level.SEVERE, null, ex);
            }
            return in;            
        }
        
        //Function to insert the key-offset in this node after split in the sub-tree below 
        void insert_sub(String key, long offset){
            int pos = Collections.binarySearch(key_list, key);
            int pos_subptr;
            if(pos >= 0) {
                System.out.println("Special case");
            } else {
                pos_subptr = -(pos + 1);
                System.out.println("inserting key-" + key + " to internal node after subtree/child splitted");
                key_list.add(pos_subptr, key);
                sub_ptr.add(pos_subptr + 1, offset);
                //If the child is not a leaf node, delete the duplicate entry in the new-child node
            }
        }
        
        @Override
        boolean is_full() {
            if(sub_ptr.size() > order) return true;
            else return false;
        }
        
        @Override
        node n_split() {
            int mid = key_list.size()/2, end = key_list.size();
            int_nd temp = new int_nd();
                    
            temp.key_list.addAll(key_list.subList(mid, end));        // add keys from mid till end to the new internal node 
            temp.sub_ptr.addAll(sub_ptr.subList(mid + 1, end + 1));  // add the sub-tree pointers to new internal node            
            key_list.subList(mid, end).clear();
            sub_ptr.subList(mid + 1, end + 1).clear();
            return temp;
        }
        
        @Override
        boolean is_leaf() {
            return false;
        }
    }
    
    //Function to calculate the order of b+ tree from a given key-size(n)
    static int find_order(int n) {
        int order, block_size = 1023;                      //1024-1; 1 byte reserved for storing leafflag for each nodes
        order = (block_size + n)/(8 + n);
        return order;
    }
    
    //Function to update parent/root node's offset in metadata
    static void update_parent(long parent_offset) throws IOException {
            raf_op.seek(260);
            raf_op.writeLong(parent_offset);
    }
       
    // Function to find a record by key, display it and return the position/offset of the record in bytes from the index file
    static long find(String key) {
        // Algorithm is already written in node's insert functions, just need to tweak a lil & use it here.
        long offset = 0;
        
        return offset;
    }
    
    // function to insert a new record in data file
    static void insert_record(String record) throws IOException {
        raf_in.seek(raf_in.length());
        raf_in.writeBytes(record);
    }
    
    // Function to list records sequentially starting from start_key. If start_key not found, display message & print next
    static void list(String start_key, int next_count) {
        // Use 'find' function's algorithm above till we reach the first leaf node while traversing the tree. Then, print
        // the next 'n' records by traversing the inter-connected leaf nodes like a linked list.
    }
    
    //Function to insert the value from data file to index file
    public static void main(String[] args) throws IOException{
        //Check if arguments passed.
        if(args.length == 0){
            System.out.println("No arguments passed.");
        }
        
        //Variables used
        long offset = 0;                                    // offset of each record from data file
        int key_len;                                        // key-length
        String key_str;
        // B_Tree_Index_Krishan obj = new B_Tree_Index_Krishan(order, args[2]);
        
        try {
            if(args[0].equals("-create")) {
                System.out.println("Creating index file ...");
                File file = new File(args[2]);
                file.delete();                              // delete the old index file if it already exists
                
                //Check the key length
                key_str = args[3];
                key_len = Integer.parseInt(key_str);        //converted the key length to integer
                if(key_len > 40 || key_len < 1){
                    System.out.println("Invalid key length passed");
                    return;
                }
                order = find_order(key_len);
                B_Tree_Index_Krishan obj = new B_Tree_Index_Krishan(args[2]);
                RandomAccessFile raf_in = new RandomAccessFile(args[1], "r");  //Data file for which index will be created
                
                //Allocate 1K block for metadata- which contains i/p file name(0-255 bytes), key-size(256th byte),parent node's offset(260-267th byte)
                byte[] meta_block = new byte[1024]; 
                raf_op.write(meta_block, 0, 1024);
                //System.out.println("Index file offset after writing 1024 bytes" + raf_op.getFilePointer());
                StringBuilder data_file = new StringBuilder(args[1]);
                for(int i = 0; i < (256 - args[1].length()); i++) {
                    data_file.append(" ");                    // Padding file_name string with " " to make length 256
                }
                raf_op.seek(0);
                raf_op.writeBytes(data_file.toString());      //data_file name written to index file      
                //System.out.println("Index file offset after writing filename:" + raf_op.getFilePointer());
                raf_op.writeInt(key_len);                     // key_size written to index file
                raf_op.writeLong(offset);                     // parent node's offset stored    
                raf_op.writeLong(order);                      // order stored at 268-275th bytes
                
                String line, key;
                int line_no = 0;
            
                // Read each line from input data file & write the key,pointer to output index file
                while((line = raf_in.readLine()) != null){
                    key = line.substring(0, key_len);             
                    obj.parent.insert(key, offset);
                    if(dup_entry) {
                        System.out.println("Duplicate entry. Key "+ key + " already exists." + line_no);
                        dup_entry = false;
                    }
                    line_no++;
                    offset = raf_in.getFilePointer();
                }
                
                //closing the data file
                raf_in.close();
            } else if(args[0].equals("-find")) {
                key_str = args[2];    
                B_Tree_Index_Krishan obj = new B_Tree_Index_Krishan(args[1]);
                
                // Get the name of the data file from 1st 256 bytes of index file
                raf_op.seek(0);
                byte[] b = null;
                raf_op.read(b, 0, 255); 
                String data_fl = new String(b);
                RandomAccessFile raf_in = new RandomAccessFile(data_fl, "r");  //Data file opened in read-only mode
                long position = find(key_str);
                System.out.println("Record is at position: " + position);
                //closing the data file
                raf_in.close();
            } else if(args[0].equals("-insert")) {
                B_Tree_Index_Krishan obj = new B_Tree_Index_Krishan(args[1]);
                raf_op.seek(256);
                key_len = raf_op.readInt();
                key_str = args[2].substring(0, (key_len - 1));
                Long offset_insert;
                
                // Get the name of the data file from 1st 256 bytes of index file
                raf_op.seek(0);
                byte[] b = null;
                raf_op.read(b, 0, 255); 
                String data_fl = new String(b);
                RandomAccessFile raf_in = new RandomAccessFile(data_fl, "rw");  //Data file opened in read-write mode                
                
                if(find(key_str) != -1) {
                    // Record entry exists in index file,so only enter in data file
                    insert_record(args[2]);
                }
                else {
                    // Record needs to be entered in data file & index file                    
                    long offset_new = raf_in.length();            // new record will be inserted at end of data file
                    insert_record(args[2]);
                    
                    //initialise parent/root node by reading it from the metadata stored in the index file
                    raf_op.seek(260);
                    node current_parent;
                    current_parent = obj.parent.read_fromFile(raf_op.readLong());              
                    current_parent.insert(key_str, offset_new);                    
                }
                //closing the data file
                raf_in.close();
            } else if(args[0].equals("-list")) {
                B_Tree_Index_Krishan obj = new B_Tree_Index_Krishan(args[1]);

                // Get the name of the data file from 1st 256 bytes of index file
                raf_op.seek(0);
                byte[] b = null;
                raf_op.read(b, 0, 255); 
                String data_fl = new String(b);
                RandomAccessFile raf_in = new RandomAccessFile(data_fl, "r");  //Data file opened in read-only mode 
                
                String start_key = args[2];
                int next_count = Integer.parseInt(args[3]);                
                list(start_key, next_count);
                //closing the data file
                raf_in.close();                
            }
            //closing the the index file
            raf_op.close();
        } catch(EOFException exception){
            System.out.println("EOF reached");
        }
    }
}
