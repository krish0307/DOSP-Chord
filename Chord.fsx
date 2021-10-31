
#r "nuget: Akka, 1.4.25"
#r "nuget: Akka.FSharp"
#r "nuget: Akka.Remote"
#load "Util.fsx"
#load "Tracker.fsx"
open System
open Akka.Actor
open Akka.FSharp
open Util
open Tracker

type Node(uid: int , m: int) =
    inherit Actor()
    let mutable tracker:IActorRef = null
    let mutable predecessor:IActorRef = null
    let mutable successor:IActorRef = null
    let getFingerTable =
        let mutable table=Array.empty
        for i in 1..m do
            table<-[|{  Start=int (uid + pown 2 i-1)%(pown 2 m);NodeUid=Int32.MaxValue;NodeReference=null }|] |>Array.append table
        table
    let mutable fingerTable = getFingerTable

    let rec isInRange (id:int) (start:int) (ending:int)=
        if start<ending then
            id > start && id < ending
        elif start>ending then
            not (isInRange id ending start) && (id <> start) && (id<>ending)
        else
            (id <> start) && (id <> ending)
        
    let rec belongs (id:int) (start:int) (ending:int)=
            if start<ending then
                id > start && id<=ending
            else if start>ending then
                not (belongs id ending start)
            else 
                true

    
    let closestPrecedingNode (id:int)=
        let mutable value=null
        let mutable breaker = false
        let mutable count = m
        while not breaker && count >=1 do
            let isTrue:bool = isInRange fingerTable.[count-1].NodeUid uid id
            if isTrue then
                value <- fingerTable.[count-1].NodeReference
                breaker <- true
            count <- count-1
        value

    override x.OnReceive(msg) =
         match msg :?> ActionType with
            | JoinNetwork ref -> 
                tracker<-x.Sender
                if ref.Path.Name.Equals(x.Self.Path.Name) then
                    predecessor <- null
                    successor <- x.Self
                    for i in 1..m do
                        fingerTable.[i-1].NodeUid <- uid
                        fingerTable.[i-1].NodeReference <- x.Self
                    tracker <! Ack
                    getSystem.Scheduler.ScheduleTellRepeatedly(TimeSpan.FromMilliseconds(50.0),TimeSpan.FromMilliseconds(80.0),x.Self,Stabilize,x.Self);
                    getSystem.Scheduler.ScheduleTellRepeatedly(TimeSpan.FromMilliseconds(60.0),TimeSpan.FromMilliseconds(90.0),x.Self,UpdateFingerTable,x.Self);
                else
                    predecessor <- null
                    ref <! GetSucessor(uid,"join",x.Self)
                
            | GetSucessor(id,taskType,ref) ->
       
                    let succUid = int successor.Path.Name
                    if belongs id uid succUid then

                        if isNull successor then
                            let sucessorType=x.Self
                            let valid=false
                            ref<! MySucessor(sucessorType,taskType,valid)
                        else
                            let returnSuccessor=successor
                            let valid=true
                            ref<! MySucessor(returnSuccessor,taskType,valid)
                    else
                        let nextHop = closestPrecedingNode(id)
                        if not (isNull nextHop) then
                            nextHop <! GetSucessor(id,taskType,ref)
                        else
                            successor <! GetSucessor(id,taskType,ref)

            | MySucessor(succ:IActorRef,reason:String,valid:Boolean)->
                if (reason = "join") then
                    if valid then
                        successor<-succ
                    tracker<! Ack
                    getSystem.Scheduler.ScheduleTellRepeatedly(TimeSpan.FromMilliseconds(50.0),TimeSpan.FromMilliseconds(80.0),x.Self,Stabilize,x.Self);
                    getSystem.Scheduler.ScheduleTellRepeatedly(TimeSpan.FromMilliseconds(60.0),TimeSpan.FromMilliseconds(90.0),x.Self,Stabilize,x.Self);

                else
                    let index = int (reason.Split (".")).[2]
                    if valid then
                        fingerTable.[index].NodeReference <- succ
                        fingerTable.[index].NodeUid <-int succ.Path.Name
            | Stabilize ->
                successor <! FindMyPredecessor
            | FindMyPredecessor ->
                if  isNull predecessor then
                    let pre = x.Self
                    let valid = false
                    x.Sender <! GetPrecedessor(pre,valid)
                else
                    let pre = predecessor
                    let valid = true
                    x.Sender <! GetPrecedessor(pre,valid)

            | GetPrecedessor(pred:IActorRef,valid:bool) ->
                if valid then
                    let preId = int pred.Path.Name
                    let sucId = int successor.Path.Name
                    if (isInRange preId uid sucId) then
                        successor <- pred
                successor <! UpdateChord(x.Self)

            | UpdateChord(ref:IActorRef) ->
                if isNull predecessor then
                    predecessor <- ref
                else
                    let preId = int predecessor.Path.Name
                    let refId = int ref.Path.Name
                    if (isInRange refId preId uid) then
                        predecessor <-ref

            | UpdateFingerTable -> 
                let i = Random().Next(0, m)
                x.Self <! GetSucessor(fingerTable.[i].Start,"finger.update."+(string i),x.Self) 

            | ShowFingerTable ->  
                let mutable predName:string=null
                if not (isNull predecessor) then
                    predName<-predecessor.Path.Name

                x.Sender<! PrintAndStartMsg
            | StartMessaging ->
                let msgID = Util.getUID  50 m
                let initialHop = 0
                for i in 1..10 do
                    getSystem.Scheduler.ScheduleTellOnce(TimeSpan.FromMilliseconds(float 100.0),x.Self,Message(msgID,initialHop,x.Self),x.Self);
                  
            | Message(msgID:int,hopCount:int,ref:IActorRef) ->
                let visitCount = hopCount + 1
                let maximum = int (pown 2 m)
                if (visitCount = maximum) then
                    tracker <! AckAndFinish(visitCount)
                else
                    let sucId = int successor.Path.Name
                    if belongs msgID uid sucId then    
                        tracker<!AckAndFinish(visitCount)
                    else
                        let closestHop = closestPrecedingNode msgID
                        if (not (isNull closestHop)) then 
                            closestHop <! Message(msgID,visitCount,ref) 
                        else 
                            successor <! Message(msgID,visitCount,ref)

let mutable numOfNodes =
    int (string (fsi.CommandLineArgs.GetValue 1))
let mutable numOfRequests =
    int (string (fsi.CommandLineArgs.GetValue 2))

let tableSize = int(ceil (Math.Log(float numOfNodes)/Math.Log(2.0)))
let mutable uidData= []
let mutable id = -1
for i in 1..numOfNodes do
    let mutable breaker = true
    while breaker do
        id<-Util.getUID 50 tableSize
        if not (List.contains id uidData) then
            uidData<- [id] |>List.append uidData
            breaker<- false

let mutable nodeArrayOfActors = Array.empty
for i in uidData do
    nodeArrayOfActors<-[|getSystem.ActorOf(Props.Create(typeof<Node>, i, tableSize),""+ string (i))|] |>Array.append nodeArrayOfActors 

let trackerRef = getSystem.ActorOf(Props.Create(typeof<Tracker>, nodeArrayOfActors,numOfNodes,numOfRequests), "tracker")
trackerRef<!Start

Console.ReadLine() |> ignore