      /***************** Transaction class **********************/
      /*** Implements methods that handle Begin, Read, Write, ***/
      /*** Abort, Commit operations of transactions. These    ***/
      /*** methods are passed as parameters to threads        ***/
      /*** spawned by Transaction manager class.              ***/
      /**********************************************************/

      /* Required header files */
      #include <stdio.h>
      #include <stdlib.h>
      #include <sys/signal.h>
      #include "zgt_def.h"
      #include "zgt_tm.h"
      #include "zgt_extern.h"
      #include <unistd.h>
      #include <iostream>
      #include <fstream>
      #include <pthread.h>


      extern void *start_operation(long, long);  //starts opeartion by doing conditional wait
      extern void *finish_operation(long);       // finishes abn operation by removing conditional wait
      extern void *open_logfile_for_append();    //opens log file for writing
      extern void *do_commit_abort(long, char);   //commit/abort based on char value (the code is same for us)

      extern zgt_tm *ZGT_Sh;			// Transaction manager object

      FILE *logfile; //declare globally to be used by all

      /* Transaction class constructor */
      /* Initializes transaction id and status and thread id */
      /* Input: Transaction id, status, thread id */

      zgt_tx::zgt_tx( long tid, char Txstatus,char type, pthread_t thrid){
        this->lockmode = (char)' ';  //default
        this->Txtype = type; //Fall 2014[jay] R = read only, W=Read/Write
        this->sgno =1;
        this->tid = tid;
        this->obno = -1; //set it to a invalid value
        this->status = Txstatus;
        this->pid = thrid;
        this->head = NULL;
        this->nextr = NULL;
        this->semno = -1; //init to  an invalid sem value
      }

      /* Method used to obtain reference to a transaction node      */
      /* Inputs the transaction id. Makes a linear scan over the    */
      /* linked list of transaction nodes and returns the reference */
      /* of the required node if found. Otherwise returns NULL      */

      zgt_tx* get_tx(long tid1){
        zgt_tx *txptr, *lastr1;

        if(ZGT_Sh->lastr != NULL){	// If the list is not empty
            lastr1 = ZGT_Sh->lastr;	// Initialize lastr1 to first node's ptr
            for  (txptr = lastr1; (txptr != NULL); txptr = txptr->nextr)
      	    if (txptr->tid == tid1) 		// if required id is found
      	       return txptr;
            return (NULL);			// if not found in list return NULL
         }
        return(NULL);				// if list is empty return NULL
      }

      /* Method that handles "BeginTx tid" in test file     */
      /* Inputs a pointer to transaction id, obj pair as a struct. Creates a new  */
      /* transaction node, initializes its data members and */
      /* adds it to transaction list */

      void *begintx(void *arg){
        //Initialize a transaction object. Make sure it is
        //done after acquiring the semaphore for the tm and making sure that
        //the operation can proceed using the condition variable. when creating
        //the tx object, set the tx to TR_ACTIVE and obno to -1; there is no
        //semno as yet as none is waiting on this tx.

          struct param *node = (struct param*)arg;// get tid and count
          start_operation(node->tid, node->count);

          zgt_p(0);        // Lock Tx manager; Add node to transaction list

          // Create new tx node - long tid, char Txstatus, char type, pthread_t thrid
          zgt_tx *tx = new zgt_tx(node->tid,TR_ACTIVE, node->Txtype, pthread_self());

          //Wring into the log file..
          open_logfile_for_append();
          fprintf(logfile, "T%d\t%c \tBeginTx\n", node->tid, node->Txtype);	// Write log record and close
          fflush(logfile);

          //Setting nextPtr to last value of ZGT_Sh(null) intially
          tx->nextr = ZGT_Sh->lastr;
          // Linking ZGT_Sh - TX1
          ZGT_Sh->lastr = tx;

          zgt_v(0); 			// Release tx manager

        finish_operation(node->tid);
        pthread_exit(NULL);				// thread exit

      }

      /* Method to handle Readtx action in test file    */
      /* Inputs a pointer to structure that contans     */
      /* tx id and object no to read. Reads the object  */
      /* if the object is not yet present in hash table */
      /* or same tx holds a lock on it. Otherwise waits */
      /* until the lock is released */


      // critical section lock soon
      void *readtx(void *arg){

        struct param *node = (struct param*)arg;
        start_operation(node->tid, node->count);
        zgt_p(0);       // Lock Tx manager

        //first get Current tx to check if same transaction holds the object lock
        zgt_tx *currentTx = get_tx(node->tid);

        if(currentTx != NULL && currentTx->status != 'A'){

          currentTx->print_tm();
            //check if object is there in the hash table using--find (long sgno, long obno) 
            zgt_hlink *obj_ptr;
            obj_ptr=ZGT_Ht->find(currentTx-> sgno,node->obno);

             if(currentTx->status == TR_ACTIVE){//Tx is active
               zgt_v(0);       // Release tx manager
              //Check if lock is obtained to read.
              int lockFreed = currentTx->set_lock(node->tid, currentTx-> sgno, node->obno, node->count, 'S', obj_ptr );

              //read write operation
              if(lockFreed == 1)
                currentTx->perform_readWrite(currentTx-> tid, node->obno, 'S');// Shared Mode to perform read
            }


        }else{
           zgt_v(0);       // Release tx manager
            //Transaction doesn't exist
            fprintf(logfile, "\t Transaction %d doesn't exist or aborted. \n", node->tid); // Write log record and close
            fflush(logfile);
        }
        finish_operation(node->tid);
        pthread_exit(NULL);
      }


      void *writetx(void *arg){ //do the operations for writing; similar to readTx
        struct param *node = (struct param*)arg;
        start_operation(node->tid, node->count);
        zgt_p(0);       // Lock Tx manager

        //first get Current tx to check if same transaction holds the object lock
        zgt_tx *currentTx = get_tx(node->tid);

         if(currentTx != NULL && currentTx->status != 'A'){
            //check if object is there in the hash table using--find (long sgno, long obno)
            zgt_hlink *obj_ptr;
            obj_ptr=ZGT_Ht->find(currentTx-> sgno,node->obno);

            if(currentTx->status == TR_ACTIVE){//Tx is active
              zgt_v(0);       // Release tx manager

              //Check if lock can be obtained by current Txn
              int lockFreed = currentTx->set_lock(node->tid, currentTx-> sgno, node->obno, node->count, 'X', obj_ptr);

              // write operation
              if(lockFreed == 1)
                currentTx->perform_readWrite(currentTx-> tid, node->obno, 'X');// Exclusive Mode write
            }

        }else{
           zgt_v(0);       // Release tx manager
            //Transaction doesn't exist
            fprintf(logfile, "\t Transaction %d doesn't exist or aborted. \n", node->tid); // Write log record and close
            fflush(logfile);
        }

        finish_operation(node->tid);
        pthread_exit(NULL);

      }

      void *aborttx(void *arg){
        struct param *node = (struct param*)arg;// get tid and count

        //write your code
        start_operation(node->tid, node->count);
        zgt_p(0);       // Lock Tx manager

        do_commit_abort(node->tid,'A');

        zgt_v(0);       // Release tx manager

        finish_operation(node->tid);
        pthread_exit(NULL);			// thread exit
      }

      void *committx(void *arg){
       //remove the locks before committing
        struct param *node = (struct param*)arg;// get tid and count

        //write your code
        start_operation(node->tid, node->count);
        zgt_p(0);       // Lock Tx manager

        zgt_tx *currentTx = get_tx(node->tid);
        currentTx->status = 'E';  //Change status to END

        printf("Semno-> inside committx :: %d\n", currentTx->semno);

        do_commit_abort(node->tid,'E');

        zgt_v(0);       // Release tx manager
        finish_operation(node->tid);
        pthread_exit(NULL);			// thread exit
      }

      // called from commit/abort with appropriate parameter to do the actual
      // operation. Make sure you give error messages if you are trying to
      // commit/abort a non-existant tx

      void *do_commit_abort(long t, char status){

        // Get Current Transaction to abort/commit
        zgt_tx *currentTx=get_tx(t);
        currentTx->print_tm();

        if(currentTx != NULL){

          // frees all locks held on the object by current transaction
            currentTx->free_locks();
          //v operation on semaphore
            zgt_v(currentTx->tid);
          //Remove the transaction node from TM
            int check_semno = currentTx->semno;
            currentTx->end_tx();

            //currentTx->print_tm();

          /* Tell all the waiting transactions in queue about release of locks by current Txn*/
            if(check_semno > -1){
              int numberOfTransaction = zgt_nwait(check_semno); //No of txns waiting on the current txn
              printf("numberOfTransaction:: %d currentTx->Semno %d \n", numberOfTransaction, check_semno);

              if(numberOfTransaction > 0){
                for (int i = 0; i < numberOfTransaction; ++i)
                {   zgt_v(check_semno);   } //Release all semaphores of waiting txns

                numberOfTransaction = zgt_nwait(check_semno); //Double check for any waiting txns
                printf("numberOfTransaction:: %d currentTx->Semno %d \n", numberOfTransaction, check_semno);
              }
            }else{
              printf("\ncheck_semno is -1\n");
            }


          // Commiting or Aborting the Transaction
            if(status== 'A'){
              fprintf(logfile, "T%d\t  \tAbortTx \t \n",t);
            }else{
              fprintf(logfile, "T%d\t  \tCommitTx \t \n",t);
            }
            fflush(logfile);

        }else{
          //Transaction doesn't exist
            fprintf(logfile, "\t Transaction %d doesn't exist. \n", t);
            fflush(logfile);
        }
      }

      int zgt_tx::remove_tx ()
      {
        //remove the transaction from the TM

        zgt_tx *txptr, *lastr1;
        lastr1 = ZGT_Sh->lastr;
        for(txptr = ZGT_Sh->lastr; txptr != NULL; txptr = txptr->nextr){	// scan through list
      	  if (txptr->tid == this->tid){		// if req node is found
      		 lastr1->nextr = txptr->nextr;	// update nextr value; done
      		 //delete this;
               return(0);
      	  }
      	  else lastr1 = txptr->nextr;			// else update prev value
         }
        fprintf(logfile, "Trying to Remove a Tx:%d that does not exist\n", this->tid);
        fflush(logfile);
        printf("Trying to Remove a Tx:%d that does not exist\n", this->tid);
        fflush(stdout);
        return(-1);
      }

      /* this method sets lock on objno1 with lockmode1 for a tx in this*/

      int zgt_tx::set_lock(long tid1, long sgno1, long obno1, int count, char lockmode1, zgt_hlink *obj_ptr){
        //if the thread has to wait, block the thread on a semaphore from the
        //sempool in the transaction manager. Set the appropriate parameters in the
        //transaction list if waiting.
        //if successful  return(0);

        //write your code

        bool lock = false;

        while(!lock){
           //Check the access mode --- Txtype   --- R(READ)/W(Write)
           zgt_p(0);
           zgt_tx *currentTx = get_tx(tid1);

           if(obj_ptr == NULL){
              //Object is not present in the Hash table , we could insert it and grant the lock
              ZGT_Ht->add(currentTx, currentTx->sgno, obno1, currentTx->lockmode);

              currentTx->status = 'P';  //Set txn to ACTIVE
              //currentTx->semno = -1;
              zgt_v(0);       // Release tx manager
              return(1);

            }else{
                //Current transaction already has the lock and is trying to access the object
                 if(obj_ptr->tid == tid1){
                    currentTx->status = 'P';
                    //currentTx->semno = -1;
                    zgt_v(0);
                    return(1);
                 }else{
                     zgt_tx *secondaryTx = get_tx(obj_ptr->tid);  //Txn holding the object
                     zgt_hlink *otherTx = others_lock(obj_ptr, sgno1, obno1);  //Other txns wanting the object.

                     if(currentTx->Txtype=='R' && secondaryTx->Txtype=='R' && otherTx->lockmode!='X'){
                      //Check if current txn and txn holding object have shared read.
                      //Also check if any other txn is waiting on the object for a write. This takes care that txn doesnt unfairly wait.
                        lock = true;  //Lock granted

                        if(currentTx->head == NULL){
                          //Add current transaction to object in hash table, as it has no pointers to hash table
                            ZGT_Ht->add(currentTx, currentTx->sgno, obno1, currentTx->lockmode);
                            obj_ptr=ZGT_Ht->find(currentTx-> sgno, obno1);
                            //printf("In here new obj head\n");
                            currentTx->head = obj_ptr;            //Point the head to this object node
                            currentTx->status = 'P';
                            //currentTx->semno = -1;
                            zgt_v(0);
                            return(1);
                        }else{
                          //iterate to the end of object list to add new required object node at the end
                          zgt_hlink *temp = currentTx->head;
                          while(temp->nextp != NULL){
                            temp = temp->nextp;
                          }
                          temp->nextp = obj_ptr;               // Point the nextp of last object node to the new node

                          currentTx->status = 'P';
                          //currentTx->semno = -1;
                          zgt_v(0);
                          return(1);
                        }
                      }
                      else{
                          //If one of the transaction is not in shared mode
                           currentTx->status = 'W';         //Make status of current txn as WAIT
                           currentTx->obno = obno1;
                           currentTx->lockmode = lockmode1;
                           if(get_tx(obj_ptr->tid))
                           currentTx->setTx_semno(obj_ptr->tid, obj_ptr->tid); //Set semaphore on current tx
                                                                             //so that txn holding object knows a new txn is waiting for the object.
                           else{
                            //In case txn holding object ceases, grant lock.
                             currentTx->status = 'P';
                             zgt_v(0);
                             return(1);

                           }

                           printf("Tx %d is waiting on:T%d\n", currentTx->tid, obj_ptr->tid);
                           currentTx->print_tm();
                           zgt_v(0);
                           zgt_p(obj_ptr->tid); //Hold txn with the object.
                           lock = false;
                          }
                  }// different thread hold Object
              }//object does exist
        }//while end
      }

      // this part frees all locks owned by the transaction
      // Need to free the thread in the waiting queue
      // try to obtain the lock for the freed threads
      // if the process itself is blocked, clear the wait and semaphores

      int zgt_tx::free_locks()
      {
        zgt_hlink* temp = head;  //first obj of tx

        open_logfile_for_append();

        for(temp;temp != NULL;temp = temp->nextp){	// SCAN Tx obj list

            fprintf(logfile, "%d : %d \t", temp->obno, ZGT_Sh->objarray[temp->obno]->value);
            fflush(logfile);

            if (ZGT_Ht->remove(this,1,(long)temp->obno) == 1){
          	   printf(":::ERROR:node with tid:%d and onjno:%d was not found for deleting", this->tid, temp->obno);		// Release from hash table
          	   fflush(stdout);
            }
            else {
      #ifdef TX_DEBUG
      	   printf("\n:::Hash node with Tid:%d, obno:%d lockmode:%c removed\n",
                                  temp->tid, temp->obno, temp->lockmode);
      	   fflush(stdout);
      #endif
            }
          }
        fprintf(logfile, "\n");
        fflush(logfile);

        return(0);
      }

      // CURRENTLY Not USED
      // USED to COMMIT
      // remove the transaction and free all associate dobjects. For the time being
      // this can be used for commit of the transaction.

      int zgt_tx::end_tx()  //2014: not used
      {
        zgt_tx *linktx, *prevp;

        linktx = prevp = ZGT_Sh->lastr;

        while (linktx){
          if (linktx->tid  == this->tid) break;
          prevp  = linktx;
          linktx = linktx->nextr;
        }
        if (linktx == NULL) {
          printf("\ncannot remove a Tx node; error\n");
          fflush(stdout);
          return (1);
        }
        if (linktx == ZGT_Sh->lastr) ZGT_Sh->lastr = linktx->nextr;
        else {
          prevp = ZGT_Sh->lastr;
          while (prevp->nextr != linktx) prevp = prevp->nextr;
          prevp->nextr = linktx->nextr;
        }
      }

      // currently not used
      int zgt_tx::cleanup()
      {
        return(0);

      }

      // check which other transaction has the lock on the same obno
      // returns the hash node
      zgt_hlink *zgt_tx::others_lock(zgt_hlink *hnodep, long sgno1, long obno1)
      {
        zgt_hlink *ep;
        ep=ZGT_Ht->find(sgno1,obno1);
        while (ep)				// while ep is not null
          {
            if ((ep->obno == obno1)&&(ep->sgno ==sgno1)&&(ep->tid !=this->tid))
      	return (ep);			// return the hashnode that holds the lock
            else  ep = ep->next;
          }
        return (NULL);			//  Return null otherwise

      }

      // routine to print the tx list
      // TX_DEBUG should be defined in the Makefile to print

      void zgt_tx::print_tm(){

        zgt_tx *txptr;

      #ifdef TX_DEBUG
        printf("printing the tx list \n");
        printf("Tid\tTxType\tThrid\t\tobjno\tlock\tstatus\tsemno\n");
        fflush(stdout);
      #endif
        txptr=ZGT_Sh->lastr;
        while (txptr != NULL) {
      #ifdef TX_DEBUG
          printf("%d\t%c\t%d\t%d\t%c\t%c\t%d\n", txptr->tid, txptr->Txtype, txptr->pid, txptr->obno, txptr->lockmode, txptr->status, txptr->semno);
          fflush(stdout);
      #endif
          txptr = txptr->nextr;
        }
        fflush(stdout);
      }

      //currently not used
      void zgt_tx::print_wait(){

        //route for printing for debugging

        printf("\n    SGNO        TxType       OBNO        TID        PID         SEMNO   L\n");
        printf("\n");
      }
      void zgt_tx::print_lock(){
        //routine for printing for debugging

        printf("\n    SGNO        OBNO        TID        PID   L\n");
        printf("\n");

      }

      // routine to perform the acutual read/write operation
      // based  on the lockmode
      void zgt_tx::perform_readWrite(long tid,long obno, char lockmode){

        // write your code
       // write your code
         zgt_p(0);        // Lock Tx manager;
         zgt_tx *txptr=get_tx(tid);
         int i;
         if(lockmode == 'S'){  //Read only mode
          //While read decrement object value by 1 for read
        ZGT_Sh->objarray[obno]->value--;

          //Write to log file
          fprintf(logfile, "T%d\t  \tReadTx \t\t %d:%d:%d  \t\t ReadLock \t Granted \t%c\n",tid,obno,ZGT_Sh->objarray[obno]->value,ZGT_Sh->optime[tid],txptr->status);  // Write log record and close
          fflush(logfile);
          zgt_v(0);       // Release tx manager
        }
         else if(lockmode == 'X'){ //Read-Write mode
         //While writing increment object value by 1
          ZGT_Sh->objarray[obno]->value++;

          //Write to log file
          fprintf(logfile, "T%d\t  \tWriteTx \t %d:%d:%d  \t\t WriteLock \t Granted \t%c\n",tid,obno,ZGT_Sh->objarray[obno]->value,ZGT_Sh->optime[tid],txptr->status);  // Write log record and close
          fflush(logfile);
          zgt_v(0);       // Release tx manager
        }
         else {
           printf("\n Error no lockmode set for tx \n");
           zgt_v(0);      // Release tx manager
        }

      }

      // routine that sets the semno in the Tx when another tx waits on it.
      // the same number is the same as the tx number on which a Tx is waiting
      int zgt_tx::setTx_semno(long tid, int semno){
        zgt_tx *txptr;

        txptr = get_tx(tid);
        if (txptr == NULL){
          printf("\n:::ERROR:Txid %d wants to wait on sem:%d of tid:%d which does not exist\n", this->tid, semno, tid);
          fflush(stdout);
          return(-1);
        }
        if (txptr->semno == -1){
          printf("txptr->semno : %d sem:%d tid: %d\n", txptr->semno , semno, tid);
          txptr->semno = semno;

          printf("txptr->semno : %d sem:%d tid: %d\n", txptr->semno , semno, tid);
          return(0);
        }
        else if (txptr->semno != semno){
      #ifdef TX_DEBUG
          printf(":::ERROR Trying to wait on sem:%d, but on Tx:%d\n", semno, txptr->tid);
          fflush(stdout);
      #endif
          exit(1);
        }
        return(0);
      }

      // routine to start an operation by checking the previous operation of the same
      // tx has completed; otherwise, it will do a conditional wait until the
      // current thread signals a broadcast

      void *start_operation(long tid, long count){

        pthread_mutex_lock(&ZGT_Sh->mutexpool[tid]);	// Lock mutex[t] to make other
        // threads of same transaction to wait

        while(ZGT_Sh->condset[tid] != count)		// wait if condset[t] is != count
          pthread_cond_wait(&ZGT_Sh->condpool[tid],&ZGT_Sh->mutexpool[tid]);

      }

      // Otherside of the start operation;
      // signals the conditional broadcast

      void *finish_operation(long tid){
        ZGT_Sh->condset[tid]--;	// decr condset[tid] for allowing the next op
        pthread_cond_broadcast(&ZGT_Sh->condpool[tid]);// other waiting threads of same tx
        pthread_mutex_unlock(&ZGT_Sh->mutexpool[tid]);
      }

      void *open_logfile_for_append(){

        if ((logfile = fopen(ZGT_Sh->logfile, "a")) == NULL){
          printf("\nCannot open log file for append:%s\n", ZGT_Sh->logfile);
          fflush(stdout);
          exit(1);
        }
      }
