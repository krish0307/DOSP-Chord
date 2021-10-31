module Util

#r "nuget: Akka, 1.4.25"
#r "nuget: Akka.FSharp"
#r "nuget: Akka.Remote"
open System.Text
open System.Security.Cryptography
open System
open Akka.Actor

type FingerTableInfo = {
  mutable Start:int;
  mutable NodeUid:int;
  mutable NodeReference:IActorRef
}

type ActionType =
    | JoinNetwork of IActorRef 
    | GetSucessor of int*String*IActorRef
    | MySucessor of IActorRef*String*Boolean
    | FindMyPredecessor
    | GetPrecedessor of IActorRef*Boolean
    | UpdateChord of IActorRef
    | UpdateFingerTable
    | Message of int*int*IActorRef
    | ShowFingerTable
    | Stabilize
    | StartMessaging

type TrackerActions =
    | Start
    | Ack
    | AckAndFinish of int
    | PrintAndStartMsg


let randomAlphanumericString (length:int)=
    let chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
    let random = Random()
    seq {
        for i in {0..length-1} do
            yield chars.[random.Next(chars.Length)]
    } |> Seq.toArray |> (fun x -> String(x))
 
let getUID (length:int) (min:int)=
    let randomStr = randomAlphanumericString length
    let byteArray = (Encoding.UTF8.GetBytes randomStr)
    let sha256Val = SHA256.Create().ComputeHash(byteArray)
    let str:string =
            Array.map (fun (x : byte) -> String.Format("{0:x2}", x)) sha256Val
            |> String.concat String.Empty
    let substr = str.[str.Length-6..str.Length]
    let intValue = Convert.ToInt32(substr, 16)
    let modIntValue = intValue % int (Math.Pow(2.0, float min))
    modIntValue

let system1 = ActorSystem.Create("System")

let getSystem =
    system1