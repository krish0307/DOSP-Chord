module Tracker
#r "nuget: Akka, 1.4.25"
#r "nuget: Akka.FSharp"
#r "nuget: Akka.Remote"
#load "Util.fsx"
open System
open Akka.Actor
open Akka.FSharp
open Util

type Tracker(nodes: IActorRef [], numNodes: int, numMsgs: int) =
    inherit Actor()
    let mutable joinCounter = 0
    let mutable peerCounter = 0
    let mutable hopCounter = 0
    let mutable msgCounter = 0
    override x.OnReceive(msg) =
        match msg :?> TrackerActions with
            | Start -> 
                nodes.[0]<! JoinNetwork(nodes.[0])
            | Ack ->
                joinCounter<-joinCounter+1
                if joinCounter< nodes.Length then
                    nodes.[joinCounter]<! JoinNetwork(nodes.[joinCounter-1])
                if joinCounter= nodes.Length then
                    getSystem.Scheduler.ScheduleTellOnce(TimeSpan.FromMilliseconds(100.0),nodes.[0],ShowFingerTable,x.Self);
            | PrintAndStartMsg ->
                peerCounter<-peerCounter+1
                if peerCounter< nodes.Length then
                    nodes.[peerCounter] <! ShowFingerTable
                if peerCounter=nodes.Length then
                    for i in 1..nodes.Length do
                        nodes.[i-1]<!StartMessaging
            | AckAndFinish hopCount-> 
                msgCounter<-msgCounter+1
                hopCounter<-hopCount+hopCounter
                if msgCounter=(numNodes*numMsgs) then
                    printfn "hopCounter %d" hopCounter
                    printfn "Avg hops per msg is %d "  (hopCounter/ (numNodes*numMsgs))
                    Environment.Exit(0)