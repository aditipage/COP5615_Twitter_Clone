open Suave
open Suave.Http
open Suave.Operators
open Suave.Filters
open Suave.Successful
open Suave.Files
open Suave.RequestErrors
open Suave.Logging
open Suave.Utils

open System
open System.Net

open Suave.Sockets
open Suave.Sockets.Control
open Suave.WebSocket

open System
open Akka
open System.Collections.Generic
open Akka.Actor
open Akka.Configuration
open Akka.FSharp

open MathNet.Numerics.Distributions
open MathNet.Numerics.Random
open System.Data
open Linq
open System.Collections.Generic



let system = System.create "system" (Configuration.defaultConfig())

type messages = 
    | RegisterServerHandler of string*string*string*string*string
    | SimulateFollowers of list<IActorRef>* array<int>*int*int
    | AssignIDs of string*IActorRef
    | AddFollower of IActorRef*IActorRef
    | NewTweetServerHandler of string*string
    | SearchHashtag of string
    | MentionServerHandler of IActorRef
    | KeywordSearchServerHandler of string*IActorRef
    | NewRetweetServerHandler of string*string
    | SignMeInServer of string
    | SignMeOutServer of string
    | PrintHomeServerHandler of string
    | PrintPendingServerHanlder of string


type messagesTwitterMain = 
    | RegisterClientHandler of string*string*string*string*string
    | AddFollowerClient of IActorRef*IActorRef
    | NewTweetClientHandler of string*string
    | UpdateHome
    | SearchHashtagClientHandler of string
    | MentionClientHandler of IActorRef
    | KeywordSearchClientHandler of string*IActorRef
    | NewRetweetClientHandler of string*string
    | SignMeOut of string
    | PrintHomeClientHandler of string
    | PrintPendingClientHanlder of string
    | SignMeIn of string
    | Searchme of String

//sign up and login
type userSignup =
    struct
        val username: string
        val firstName: string
        val lastName: string
        val email: string
        val pw : string
    end

//login
type userLogin =
    struct
        val username: string
        val pw : string
    end

//home
type userHome =
    struct
        val originalFName: string
        val originalLName: string
        val firstName: string
        val lastName: string
        val tweet: string
        //val tweetID: string
    end

//followers
type userFollowers =
    struct
        val followerUserName: string
        //val tweetID: string
    end

//followees
type userFollowees =
    struct
        val followeeUserName: string
        //val tweetID: string
    end

// //home after retweet
// type retweet =
//     struct
//         val x: float
//         val y: float
//         val z: float
//     end

//search for hashtag
type searchHashtag =
    struct
        val firstName: string
        val lastName: string
        val tweet: string
        //val tweetID: string
    end

//search for my mentions: all tweets that contain my username mentions
type searchMention =
    struct
        val firstName: string
        val lastName: string
        val tweet: string
    end

//search for keyword
type searchKeyword =
    struct
        val firstName: string
        val lastName: string
        val tweet: string
    end

let mutable tweetID = 0
let mutable retweetID = 0
let mutable reftoID:Map<IActorRef,string> = Map.empty
let mutable IDtoref:Map<string,IActorRef> = Map.empty
let mutable userTweetMap: Map<string, Set<string>> = Map.empty
let mutable serverPendingMap:Map<IActorRef,list<(string*string*string*string*string)>> = Map.empty
let mutable serverStatus:Map<IActorRef,Boolean> = Map.empty
let mutable homeMap:Map<IActorRef,list<(string*string *string*string*string)>> = Map.empty
let mutable usernameToFullNameMap: Map<string, string*string> = Map.empty
let mutable usernameToPassword: Map<string, string> = Map.empty // for validation of login


let mutable followersMap: Map<string, Set<string>> = Map.empty
let mutable followingMap: Map<string, Set<string>> = Map.empty

let mutable actorWebsocketMap: Map<IActorRef, WebSocket> = Map.empty


let mutable mapRef: Map<int, IActorRef> = Map.empty

let dt = new DataTable()
dt.Columns.Add("Username", typeof<string>);
dt.Columns.Add("FirstName", typeof<string>);
dt.Columns.Add("LastName", typeof<string>);
dt.Columns.Add("Email", typeof<string>);
dt.Columns.Add("Password", typeof<string>);

dt.PrimaryKey <- [|dt.Columns.["Username"]|]

// for tweet table 
let tweetTable = new DataTable()
tweetTable.Columns.Add("TweetID", typeof<string>);
tweetTable.Columns.Add("Username", typeof<string>);
tweetTable.Columns.Add("Tweet", typeof<string>);

tweetTable.PrimaryKey <- [|tweetTable.Columns.["TweetID"]|]

let retweetTable = new DataTable()
retweetTable.Columns.Add("reTweetID", typeof<string>);
retweetTable.Columns.Add("Username", typeof<string>);
retweetTable.Columns.Add("TweetID", typeof<string>);

retweetTable.PrimaryKey <- [|retweetTable.Columns.["reTweetID"]|]
let mutable registerCount = 0

let register (username:string)(firstname:string)(lastname:string)(email:string)(password:string) = 
    let dr = dt.NewRow()
    dr.SetField("username",username)
    dr.SetField("FirstName", firstname)
    dr.SetField("LastName", lastname)
    dr.SetField ("Email", email)
    dr.SetField("Password",password)
    dt.Rows.Add(dr)
    serverStatus <- serverStatus.Add(IDtoref.[username], false)
    homeMap <- homeMap.Add(IDtoref.[username],[])
    userTweetMap <- userTweetMap.Add(username,Set.empty)
    usernameToPassword <- usernameToPassword.Add(username, password)
    //printfn "Working till here"
    registerCount <- registerCount + 1
    serverPendingMap <- serverPendingMap.Add(IDtoref.[username],[])
    usernameToFullNameMap <- usernameToFullNameMap.Add(username,(firstname,lastname)) 




let ws (webSocket : WebSocket) (context: HttpContext) =
  socket {
    // if `loop` is set to false, the server will stop receiving messages
    let mutable loop = true
    //actorWebsocketMap <- actorWebsocketMap.Add(IActorRef, webSocket) 
    while loop do
      // the server will wait for a message to be received without blocking the thread
      let! msg = webSocket.read()
       
      match msg with
      // the message has type (Opcode * byte [] * bool)
      //
      // Opcode type:
      //   type Opcode = Continuation | Text | Binary | Reserved | Close | Ping | Pong
      //
      // byte [] contains the actual message
      //
      // the last element is the FIN byte, explained later
      | (Text, data, true) ->
        // the message can be converted to a string
        let str = UTF8.toString data
        let mutable response = sprintf "response to %s" str
        //response <- "DONEEEEE"
        printfn "response: %s" response 


        if response.Contains("SIGNUP") then
            printfn "IN IF:signup"
            // var signupMsg = "type:SIGNUP%usrname:"+usrname+"%fname:"+fname+"%lname:"+lname+"%psw:"+psw;
            
            let temp1 = response.Split "%"
            printfn "checkpoint 1"
            //let temp2 = response.Split "%"
            let username = (temp1.[1].Split ":").[1]
            printfn "%s" username
            let firstname = (temp1.[2].Split ":").[1]
            let lastname = (temp1.[3].Split ":").[1]
            let email = (temp1.[4].Split ":").[1]
            let password = (temp1.[5].Split ":").[1]

            printfn "username %A" username
            
            register (username)(firstname)(lastname)(email)(password)


            


        elif response.Contains("LOGIN") then
            printfn "IN IF:login"
            //var loginMsg = "type:LOGIN%uname:"+username+"%pw:"+pw;
            if response.Contains("uname") then
                let temp1 = response.Split "%"
                let temp2 = temp1.[1].Split ":"
                let temp3 = temp1.[2].Split ":"
                //printfn "temp %A" temp2
                let uname = temp2.[1]
                let pw = temp3.[1]
                printfn "uname %A" uname
                printfn "pw %A" pw
                // login validation
                if usernameToPassword.ContainsKey(uname) then
                    if usernameToPassword.[uname] = pw then 
                        printfn "Login Successful"
                        let actorRef = IDtoref.[uname]
                        actorWebsocketMap <- actorWebsocketMap.Add(actorRef, webSocket)
                        //printfn "homemap of user: %A" homeMap.[IDtoref.[uname]]

                        for eachRow in homeMap.[IDtoref.[uname]] do
                            //printfn "Homemap rows: %A" eachRow

                            let maketweet (eachRow) = 
                                let x,_,_,_,_ = eachRow
                                let _,y,_,_,_ = eachRow
                                let _,_,a,_,_ = eachRow
                                let _,_,_,b,_ = eachRow
                                let _,_,_,_,t = eachRow

                                let firstName = x
                                let lastName = y
                                let originalFName = a
                                let originalLName = b
                                let tweet = t
                                let mutable str = ("type:$home$%firstName:"+firstName+"%lastName:"+lastName+"%originalFName:"+originalFName+"%originalLName:"+originalLName+ "%tweet:"+tweet+"")
                                str
                                
                                
                            let str = maketweet(eachRow)
                            
                            let byteResponse1 =
                                str
                                |> System.Text.Encoding.ASCII.GetBytes
                                |> ByteSegment
                            
                            //do! webSocket.send Text byteResponse1 true
                        
                            let sendmessage (webSocket : WebSocket)=  
                                webSocket.send Text byteResponse1 true
                            Async.RunSynchronously(sendmessage webSocket)

                        // follower
                        for eachFollower in followersMap.[uname] do
                            
                            let mutable str = ("type:$follower$%followerUserName:"+eachFollower+"")
                            
                            
                            let byteResponse1 =
                                str
                                |> System.Text.Encoding.ASCII.GetBytes
                                |> ByteSegment
                            
                            //do! webSocket.send Text byteResponse1 true

                            let sendmessage (webSocket : WebSocket)=  
                                webSocket.send Text byteResponse1 true
                            Async.RunSynchronously(sendmessage webSocket)


                        // followees
                        for eachFollowee in followingMap.[uname] do
                        
                            let mutable str = ("type:$followee$%followeeUserName:"+eachFollowee+"")
                            
                            let byteResponse1 =
                                str
                                |> System.Text.Encoding.ASCII.GetBytes
                                |> ByteSegment
                            
                            //do! webSocket.send Text byteResponse1 true

                            let sendmessage (webSocket : WebSocket)=  
                                webSocket.send Text byteResponse1 true
                            Async.RunSynchronously(sendmessage webSocket)


                        
                    else
                        printfn "Wrong password"
                else
                    printfn "Login failed"


    
        elif response.Contains("SEARCH") then
            printfn "IN IF:search"
            //var searchMsg = "type:SEARCH%toSearch:"+toSearch;
            let temp1 = response.Split "%"
            let username = (temp1.[1].Split ":").[1]
            let toSearch = (temp1.[2].Split ":").[1]

            let actorRef = IDtoref.[username]

            actorRef <! Searchme toSearch

        elif response.Contains("NEWTWEET") then
            let temp1 = response.Split "%"
            let username = (temp1.[1].Split ":").[1]
            let tweet = (temp1.[2].Split ":").[1]
            let actorRef = IDtoref.[username]
            actorRef <! NewTweetClientHandler (tweet,username)

        elif response.Contains("FOLLOW") then
            //var followMsg = "type:FOLLOW%myUser:"+myUser+"%follow:"+follow;

            let temp1 = response.Split "%"
            let username = (temp1.[1].Split ":").[1]
            let newUserID = (temp1.[2].Split ":").[1]
            let actorRef = IDtoref.[username]
            printfn "IN FOLLOW RESPONSE"
            actorRef <! AddFollowerClient (actorRef,IDtoref.[newUserID])

          
        // the response needs to be converted to a ByteSegment
        let byteResponse =
          response
          |> System.Text.Encoding.ASCII.GetBytes
          |> ByteSegment

        // the `send` function sends a message back to the client
        do! webSocket.send Text byteResponse true

      | (Close, _, _) ->
        let emptyResponse = [||] |> ByteSegment
        do! webSocket.send Close emptyResponse true

        // after sending a Close message, stop the loop
        loop <- false

      | _ -> ()
    }

/// An example of explictly fetching websocket errors and handling them in your codebase.
let wsWithErrorHandling (webSocket : WebSocket) (context: HttpContext) = 
   
   let exampleDisposableResource = { new IDisposable with member __.Dispose() = printfn "Resource needed by websocket connection disposed" }
   let websocketWorkflow = ws webSocket context
   
   async {
    let! successOrError = websocketWorkflow
    match successOrError with
    // Success case
    | Choice1Of2() -> ()
    // Error case
    | Choice2Of2(error) ->
        // Example error handling logic here
        printfn "Error: [%A]" error
        exampleDisposableResource.Dispose()
        
    return successOrError
   }

let app : WebPart = 
  choose [
    path "/websocket" >=> handShake ws
    path "/websocketWithSubprotocol" >=> handShakeWithSubprotocol (chooseSubprotocol "test") ws
    path "/websocketWithError" >=> handShake wsWithErrorHandling
    GET >=> choose [ path "/" >=> file "index.html"]
   

    NOT_FOUND "Found no handlers." ]

[<EntryPoint>]
let main _ =
    // *************************************************************** DATABLES ***************************************************************
    // for user table 
    


// *************************************************************** REGISTER ***************************************************************


    // let mutable tweetID = 0
    // let mutable retweetID = 0
    // let mutable reftoID:Map<IActorRef,string> = Map.empty
    // let mutable IDtoref:Map<string,IActorRef> = Map.empty
    // let mutable userTweetMap: Map<string, Set<string>> = Map.empty
    // let mutable serverPendingMap:Map<IActorRef,list<(string*string*string*string*string)>> = Map.empty
    // let mutable serverStatus:Map<IActorRef,Boolean> = Map.empty
    // let mutable homeMap:Map<IActorRef,list<(string*string *string*string*string)>> = Map.empty
    // let mutable usernameToFullNameMap: Map<string, string*string> = Map.empty

    let rand1 = new Random()
    //let mutable nClients = (200*20)/100
    let mutable nClients1 = 0
    let mutable count = 0
    
    // Populating user table
    
        //printfn "username pw: %A" usernameToPassword
        //printfn "working till here 2"

    let assignIDs (id:string)(ref:IActorRef) =
        //printfn "here"
        reftoID <- reftoID.Add(ref,id)
        IDtoref <- IDtoref.Add(id,ref)
         

    // let mutable followersMap: Map<string, Set<string>> = Map.empty
    // let mutable followingMap: Map<string, Set<string>> = Map.empty

    let mutable fname = ""
    let mutable lname = "" 
    let mutable tweet1 = ""  

    let mutable lnameIndex = 2
    let mutable fnameIndex = 1
    let mutable ind = 0


    let mutable hashTagMap: Map<string, Set<string>> = Map.empty
    let mutable mentionMap: Map<string, Set<string>> = Map.empty


    let mutable nameTuple = (lname, fname)


    let mutable innerMap: Map<string, List<string>> = Map.empty // map with nameTuples as keys 
    let mutable outerMap: Map<string, Map<string, List<string>>> = Map.empty // map with currUser as keys

    let mutable tweetIDIndex = 0
    let mutable tweetIndex = 2 // for retrieving tweets from tweettable 

    let nametup:(string*string) = ("","")


    // Now that we have tweetIDIndex and tweetID, we can retrieve the tweet corresponding to each followee
    let extractTweetfromFollowees(tempFolloweeSet:Set<string>) = 
        for followeeID in tempFolloweeSet do
            //printfn "Hello here"
            let mutable expr = "Username = '"+followeeID+"'"
            //printfn "expr 2 %s" expr
            let tweetRow = (tweetTable.Select(expr))    // as enumerable can be used
        
            let tweetSeq = seq{
                yield! tweetRow
            }
            let mutable tweetList= new List<String>()
            for i in tweetSeq do
                tweet1 <- (i.Field(tweetTable.Columns.Item(2)))
                tweetList.Add(tweet1)
                        // printfn "HIIII"
            if  tweetList.Count > 0 then
                innerMap <- innerMap.Add(followeeID, tweetList)

        //printMap innerMap
    let findFollowees currUser = 
        let mutable tempFolloweeSet = Set.empty
        for i in followingMap.[currUser] do
            //printfn "Follwing IDS: %s" i
            tempFolloweeSet <- tempFolloweeSet.Add(i)
        //findUserDetailsfromUserID tempFolloweeSet
        tempFolloweeSet
        //outerMap <- outerMap.Add(currUser, innerMap)

    let sendTweetToFollowers(senderID:string)(tweet:string)(tempFollowerSet:Set<string>) = 
        for followerID in tempFollowerSet do
            //printfn "Hello here"
            let mutable first_name = ""
            let mutable last_name = ""
            
            if serverStatus.[IDtoref.[followerID]] then
                //printfn "here in "
                
                //homeMap <- homeMap.Add(IDtoref.[followerID])
                let orig_list = homeMap.[IDtoref.[followerID]]
                
                first_name <- fst usernameToFullNameMap.[senderID]
                last_name <- snd usernameToFullNameMap.[senderID]
                
                let new_tweet = [("","",first_name,last_name,tweet)]
                let final_list = orig_list @ new_tweet
                //final_list <- List.rev final_list
                homeMap <- homeMap.Add(IDtoref.[followerID],final_list)
                if actorWebsocketMap.ContainsKey(IDtoref.[followerID]) then
                    let actorWebsocket = actorWebsocketMap.[IDtoref.[followerID]]
                    //for eachRow in homeMap.[IDtoref.[followerID]] do
                            //printfn "Homemap rows: %A" eachRow
                    let list = homeMap.[IDtoref.[followerID]]
                    let eachRow = list.[list.Length - 1]
                    let x,_,_,_,_ = eachRow
                    let _,y,_,_,_ = eachRow
                    let _,_,a,_,_ = eachRow
                    let _,_,_,b,_ = eachRow
                    let _,_,_,_,t = eachRow

                    let firstName = x
                    let lastName = y
                    let originalFName = a
                    let originalLName = b
                    let tweet = t
                    let mutable str = ("type:$home$%firstName:"+firstName+"%lastName:"+lastName+"%originalFName:"+originalFName+"%originalLName:"+originalLName+ "%tweet:"+tweet+"")
                    
                        
                    
                    let byteResponse1 =
                        str
                        |> System.Text.Encoding.ASCII.GetBytes
                        |> ByteSegment

                    let sendmessage (webSocket : WebSocket)=  
                        //printfn "hello" 
                        webSocket.send Text byteResponse1 true
                      
                    Async.RunSynchronously(sendmessage actorWebsocket)
                    printfn "" 
               
            else
                //printfn "here"
                if serverPendingMap.ContainsKey(IDtoref.[followerID]) then
                    //printfn "Inside if"
                //serverPendingMap <- serverPendingMap.Add([IDtoref.[followerID]],serverPendingMap.[IDtoref.[followerID]].Append(senderID,tweet))  
                    let orig_list = serverPendingMap.[IDtoref.[followerID]]
                    //printfn "Sender id is %s" senderID
                    //printfn "Original list is %A" orig_list
                    first_name <- fst usernameToFullNameMap.[senderID]
                    last_name <- snd usernameToFullNameMap.[senderID] 
                    
                    let new_tweet = [("","",first_name,last_name,tweet)]
                    let mutable final_list = orig_list @ new_tweet
                    //final_list <- List.rev final_list
                    //printfn "FInal list is %A" final_list
                  
                    serverPendingMap <- serverPendingMap.Add(IDtoref.[followerID],final_list)
                    //printfn "Map for 1 %A " serverPendingMap.[IDtoref.[followerID]]
                    
    let addFollowerServer(follower:IActorRef)(followee:IActorRef) =
        printfn "IN ADD FOLLOWER SERVER"

        let followerID = reftoID.[follower]
        let followeeID = reftoID.[followee]
        followingMap <-followingMap.Add(reftoID.[follower],followingMap.[reftoID.[follower]].Add(followeeID))
        followersMap <-followersMap.Add(reftoID.[followee],followersMap.[reftoID.[followee]].Add(followerID))
        printfn "eachfollowee: %i" actorWebsocketMap.Count
        //for eachFollowee in followingMap.[followerID] do
            
        if actorWebsocketMap.ContainsKey(follower) then   
            //printfn "ALL GOOD"         
            let mutable str = ("type:$follow$%followeeUserName:"+followeeID)
            
            let byteResponse1 =
                str
                |> System.Text.Encoding.ASCII.GetBytes
                |> ByteSegment
            
            //do! webSocket.send Text byteResponse1 true

            let sendmessage (webSocket : WebSocket)=  
                webSocket.send Text byteResponse1 true
            Async.RunSynchronously(sendmessage actorWebsocketMap.[follower])
            printfn ""    
            
    let sendReTweetToFollowers(senderID:string)(tweetID:string)(tempFollowerSet:Set<string>) = 
        //printfn "TODO"
        //printfn "no of followers %i" tempFollowerSet.Count
        for followerID in tempFollowerSet do
            let mutable first_name = ""
            let mutable last_name = ""
            let mutable orig_first_name = ""
            let mutable orig_last_name = ""
            let mutable tweet = ""
            //printfn "Current followerID is %s" followerID

            //printfn "Server status map %A" serverStatus
            if serverStatus.[IDtoref.[followerID]] then
                //printfn "here in "
                
                //homeMap <- homeMap.Add(IDtoref.[followerID])
                let orig_list = homeMap.[IDtoref.[followerID]]
                //printfn "what is the matter?"

     
                first_name <- fst usernameToFullNameMap.[senderID]
                last_name <- snd usernameToFullNameMap.[senderID]
                
                
                let mutable exprToFindTweet = "tweetID = '"+tweetID+"'"
                let mutable foundTweetRow = (tweetTable.Select(exprToFindTweet)).AsEnumerable()
                let tweetRowSeq = seq { yield! foundTweetRow}
                for j in tweetRowSeq do
                    let orig_UserID = j.Field(tweetTable.Columns.Item(0)).ToString()
                    
                    orig_first_name <- fst usernameToFullNameMap.[orig_UserID]
                    orig_last_name <- snd usernameToFullNameMap.[orig_UserID]

                
                    tweet <- j.Field(tweetTable.Columns.Item(2)).ToString()
                let new_tweet = [(first_name,last_name, orig_first_name,orig_last_name,tweet)]
                let final_list =  orig_list @ new_tweet
                
                homeMap <- homeMap.Add(IDtoref.[followerID],final_list)
            else
                if serverPendingMap.ContainsKey(IDtoref.[followerID]) then
                    //serverPendingMap <- serverPendingMap.Add([IDtoref.[followerID]],serverPendingMap.[IDtoref.[followerID]].Append(senderID,tweet))  
                    let orig_list = serverPendingMap.[IDtoref.[followerID]]
                    
                    first_name <- fst usernameToFullNameMap.[senderID]
                    last_name <- snd usernameToFullNameMap.[senderID]
                    let mutable exprToFindTweet = "tweetID = '"+tweetID+"'"
                    let mutable foundTweetRow = (tweetTable.Select(exprToFindTweet)).AsEnumerable()
                    let tweetRowSeq = seq { yield! foundTweetRow}
                    for j in tweetRowSeq do
                        let orig_UserID = j.Field(tweetTable.Columns.Item(0)).ToString()
                        
                        orig_first_name <- fst usernameToFullNameMap.[orig_UserID]
                        orig_last_name <- snd usernameToFullNameMap.[orig_UserID]  
                        tweet <- j.Field(tweetTable.Columns.Item(2)).ToString()
                            
                    let new_tweet = [(first_name,last_name,orig_first_name,orig_last_name,tweet)]
                    let final_list =  orig_list @ new_tweet
                    serverPendingMap <- serverPendingMap.Add(IDtoref.[followerID],final_list)
                else 
                    printfn "nothing"
            
        
    let tweetParse (tweetID: string) (tweet: string) (userID:string)= 
        let splitLine = (fun (line : string) -> Seq.toList (line.Split ' '))
        //printfn "Inside tweetParse method"
        let res = splitLine tweet
        for value in res do
            if value.Contains("#") then 
                // add directly to map
                if hashTagMap.ContainsKey(value.[1..value.Length]) then
                    hashTagMap <- hashTagMap.Add(value.[1..value.Length],  hashTagMap.[value.[1..value.Length]].Add(tweetID))
                else
                    hashTagMap <- hashTagMap.Add(value.[1..value.Length], Set.empty)
                    hashTagMap <- hashTagMap.Add(value.[1..value.Length],  hashTagMap.[value.[1..value.Length]].Add(tweetID))
            if value.Contains("@") then
                // add to map 
                if (IDtoref.ContainsKey(value.[1..value.Length])) then
                    if mentionMap.ContainsKey(value.[1..value.Length]) then
                        mentionMap <- mentionMap.Add(value.[1..value.Length],  mentionMap.[value.[1..value.Length]].Add(tweetID))
                    else
                        mentionMap <- mentionMap.Add(value.[1..value.Length], Set.empty)
                        mentionMap <- mentionMap.Add(value.[1..value.Length],  mentionMap.[value.[1..value.Length]].Add(tweetID))
            //if userTweetMap.ContainsKey(userID) then
            userTweetMap <- userTweetMap.Add(userID,  userTweetMap.[userID].Add(tweetID))

    let searchHashtag(valtosearch:string)  (currUser: IActorRef)= 
        let mutable tempTweetSearchList = List.empty
        let mutable finalSearchFolloweeIDTweetMap: Map<string, List<string>> = Map.empty
        let mutable resMap: Map<string, Set<string>> = Map.empty
        let mutable result : Map<string,Set<( string*string)>> = Map.empty 
        //let mutable usersList = List.empty
        if hashTagMap.ContainsKey( (valtosearch.[1..(valtosearch.Length-1)]) ) then
            resMap <- resMap.Add(valtosearch, hashTagMap.[(valtosearch.[1..(valtosearch.Length-1)])])
            //printfn "resMap #: %A" resMap
            // here we finding tweets corresponding to each TweetID
            for twID in resMap.[valtosearch] do
                let mutable expr = "TweetID = '"+twID+"'"
                let mutable foundTweetRows = (tweetTable.Select(expr)).AsEnumerable()
                let Tseq = seq{
                    //printfn "HEYYYYYYYYY"
                    yield! foundTweetRows
                }
                let mutable origList = List.empty 
                for i in Tseq do        
                    origList <- tempTweetSearchList
                    let newTweet = [i.Field(tweetTable.Columns.Item(tweetIndex))]
                    let userID = i.Field(tweetTable.Columns.Item(1))
                    //let mutable expr = "username = '"+userID+"'"
                    let first_name = fst usernameToFullNameMap.[userID]
                    let last_name = snd usernameToFullNameMap.[userID]
                   
                    if result.ContainsKey(valtosearch) then
                        result <- result.Add(valtosearch,result.[valtosearch].Add(first_name + " " + last_name,newTweet.[0]))
                    else
                        result <- result.Add(valtosearch,Set.empty)
                        result <- result.Add(valtosearch,result.[valtosearch].Add(first_name + " " + last_name,newTweet.[0]))
                    ()
                    
                    tempTweetSearchList <- (origList @ newTweet)
                     
            let temp = result.[valtosearch].AsEnumerable()
            let Tseq2 = seq{
                yield! temp
            }
            printfn "%A" Tseq2.Count
            for l_item in Tseq2 do
                let x,_ = l_item
                let _,t = l_item

                let fullname = x
                let tweet = t

                let mutable str = ("type:$search$%fullName:"+fullname+"%tweet:"+tweet)

                let byteResponse1 =
                    str
                    |> System.Text.Encoding.ASCII.GetBytes
                    |> ByteSegment

                let sendmessage (webSocket : WebSocket)=  
                    printfn "hello" 
                    webSocket.send Text byteResponse1 true
                Async.RunSynchronously(sendmessage actorWebsocketMap.[currUser])
                
    let searchMentions(currUser:IActorRef)= 
        let mutable tempTweetSearchList:list<(string*string*string)> = List.empty
        let valTosearch = reftoID.[currUser]
        let mutable resMap: Map<string, Set<string>> = Map.empty
        printfn "Val to search is %s" valTosearch
        if mentionMap.ContainsKey(valTosearch) then 
            resMap <- resMap.Add(valTosearch, mentionMap.[valTosearch])
            tempTweetSearchList <- List.empty
            for twID in resMap.[valTosearch] do
                let mutable expr = "TweetID = '"+twID+"'"
                let mutable foundTweetRows = (tweetTable.Select(expr)).AsEnumerable()
                let Tseq = seq{
                    yield! foundTweetRows
                }       
                //tempTweetSearchList <- List.empty
                let mutable origList = List.empty
                // now we already have the tweetID
                for i in Tseq do
                    origList <- tempTweetSearchList
                    let newTweet = i.Field(tweetTable.Columns.Item(tweetIndex))
                    let userID = i.Field(tweetTable.Columns.Item(1))
                    let first_name = fst usernameToFullNameMap.[userID]
                    let last_name = snd usernameToFullNameMap.[userID]
                   
                    let newTweetTuple = [(first_name,last_name,newTweet)]
                    tempTweetSearchList <- (origList @ newTweetTuple)
                    //tempTweetSearchList <- tempTweetSearchSet.Add(i.Field(tweetTable.Columns.Item(tweetIndex)))

            for l_item in tempTweetSearchList do
                let firstname,_,_ = l_item
                let _,lastname,_ = l_item
                let _,_, t = l_item 

                let fullname = firstname + " " +  lastname
                let tweet = t

                let mutable str = ("type:$search$%fullName:"+fullname+"%tweet:"+tweet)

                let byteResponse1 =
                    str
                    |> System.Text.Encoding.ASCII.GetBytes
                    |> ByteSegment

                let sendmessage (webSocket : WebSocket)=  
                    webSocket.send Text byteResponse1 true
                Async.RunSynchronously(sendmessage actorWebsocketMap.[currUser])

           


    let searchKeyword (valToSearch: string) (currUser: IActorRef) =
        //printfn "HEREEEEEEEEEEEEEEEE"
        let homeTupleList = homeMap.[currUser]
        //printfn "HometupleList: %A" homeTupleList
        let mutable resList= new List<string*string*string*string*string>()
        for tup in homeTupleList do
            let _,_,_,_,tweet = tup
            if tweet.Contains(valToSearch) then
                resList.Add(tup)
                //resList <- [tup] |> List.append resList
        for l_item in resList do
                let x,_,_,_,_ = l_item
                let _,y,_,_,_ = l_item
                let _,_,a,_,_ = l_item
                let _,_,_,b,_ = l_item
                let _,_,_,_,t = l_item

                let mutable str = ("type:$se1$%sid:"+ x + " " + y + "%original:" + a + " " + b + "%tweet:" + t)
                let byteResponse1 =
                    str
                    |> System.Text.Encoding.ASCII.GetBytes
                    |> ByteSegment

                let sendmessage (webSocket : WebSocket)=  
                    webSocket.send Text byteResponse1 true
                Async.RunSynchronously(sendmessage actorWebsocketMap.[currUser])

       

    let findUserDetailsfromUserID(tempFolloweeSet:Set<string>) = 
        for usrname in tempFolloweeSet do
            let mutable expr = "'username' = '"+usrname+"'"
            //printfn "expr 1 %s" expr
            let mutable fullNameRow = (dt.Select("Username = '"+usrname+"'")).AsEnumerable()
            let fullNameSeq = seq{
                yield! fullNameRow
            }

            for i in fullNameSeq do
                for j in 0..dt.Columns.Count-1 do
                        lname <- (i.Field(dt.Columns.Item(lnameIndex)))
                        fname <- (i.Field(dt.Columns.Item(fnameIndex)))
                        nameTuple <- (lname, fname)
            usernameToFullNameMap <- usernameToFullNameMap.Add(usrname, nameTuple)
        extractTweetfromFollowees tempFolloweeSet

    let signInAUser(userID:string) = 
        serverStatus <- serverStatus.Add(IDtoref.[userID],true)
        let orig_pending_list = serverPendingMap.[IDtoref.[userID]]
        let orig_home_list = homeMap.[IDtoref.[userID]]
        let mutable new_list = orig_pending_list @ orig_home_list
        //new_list <- List.rev(new_list)
        homeMap <- homeMap.Add(IDtoref.[userID],new_list)
        serverPendingMap <- serverPendingMap.Add(IDtoref.[userID],[])
        
    let signOffAUser(userID:string) = 
        serverStatus <- serverStatus.Add(IDtoref.[userID],false) 


    let printPendingServerHanlder userID = 
        let userref = IDtoref.[userID]
        let mutable tweetsTuple = serverPendingMap.[userref]
        //printfn "Inside pending map method"
        //let index = 5
        tweetsTuple <- List.rev tweetsTuple
        let index = tweetsTuple.Length
        //printfn "Total length is %i" index
        //printfn "Total length for home %i" index
        if index > 4 then
        
            for i in 0..4 do
        
        //tweetsTuple.[i]
        //for tuple in tweetsTuple do 
            //let _,_,_,_,tweet = tup
                let x,_,_,_,_ = tweetsTuple.[i]
                let _,y,_,_,_ = tweetsTuple.[i]
                let _,_,a,_,_ = tweetsTuple.[i]
                let _,_,_,b,_ = tweetsTuple.[i]
                let _,_,_,_,t = tweetsTuple.[i]

                if x.Equals("") then
                    printf "%s " a
                    printfn "%s" b
                    printfn "%s" t
                else
                    printfn "This is a retweet"
                    printf "%s " x
                    printfn "%s" y
                    printfn "Original User"

                    printf "%s " a
                    printfn "%s" b
                    printfn "%s" t



    let printHomeServerHanlder userID = 
        let userref = IDtoref.[userID]
        let mutable tweetsTuple = homeMap.[userref]
        //let index = 5
        tweetsTuple <- List.rev tweetsTuple
        let index = tweetsTuple.Length  
        //printfn "Total length for home %i" index
        if index > 4 then
            for i in 0..4 do
                let x,_,_,_,_ = tweetsTuple.[i]
                let _,y,_,_,_ = tweetsTuple.[i]
                let _,_,a,_,_ = tweetsTuple.[i]
                let _,_,_,b,_ = tweetsTuple.[i]
                let _,_,_,_,t = tweetsTuple.[i]

                if x.Equals("") then
                    printf "%s " a
                    printfn "%s" b
                    printfn "%s" t
                else
                    printfn "This is a retweet"
                    printf "%s " x
                    printfn "%s" y
                    printfn "Original User"
                    printf "%s " a
                    printfn "%s" b
                    printfn "%s" t
       
    let findFollowers userID tweet  = 
        let mutable tempFollowerSet = Set.empty
       
        for i in followersMap.[userID] do
            //printfn "Follwing IDS: %s" i
            
            tempFollowerSet <- tempFollowerSet.Add(i)
        
        sendTweetToFollowers userID tweet tempFollowerSet

    let findFollowersForRetweeting newUserID tweetID = 
        let mutable tempFollowerSet = Set.empty
        for i in followersMap.[newUserID] do
                //printfn "Follwing IDS: %s" i
            tempFollowerSet <- tempFollowerSet.Add(i)

        sendReTweetToFollowers newUserID tweetID tempFollowerSet
        //printfn "all clear"

    let populatingTweetTable (username:string)(tweet:string) = 
        tweetID <- tweetID + 1
        
        let dr = tweetTable.NewRow()
        dr.SetField("TweetID",(tweetID.ToString()))
        dr.SetField("Username", username)
        dr.SetField("Tweet", tweet)
        tweetTable.Rows.Add(dr)
        

    let printTweetTable (tweet:string) = 
        
        tweetTable.AsEnumerable()
        |> Seq.iter(fun x -> printfn "%s" (String.Join(" ", x.ItemArray)))


    let printRetweetTable(tweet:string) = 
        retweetTable.AsEnumerable()
        |> Seq.iter(fun x -> printfn "%s" (String.Join(" ", x.ItemArray)))

    let simulateFollowers (ref:list<IActorRef>)(distArray:array<int>) (nActors: int)(clients:int) =
        //printfn "%A" reftoID
        nClients1 <- clients
        for i in 0..nActors-1 do
            followersMap<- followersMap.Add(reftoID.[ref.[i]], Set.empty)
        //printfn "simulateFollowers checkpoint 1"   	
        for i in 0..nActors-1 do
            followingMap<- followingMap.Add(reftoID.[ref.[i]], Set.empty)

        for i in 0..ref.Length-1 do
            //printfn "%i" array.[i]	
            let mutable tempSet:Set<string> = Set.empty
            for j in 0..distArray.[i]-1 do
                //printfn "value of j is %i" j	
                let mutable x = rand1.Next(1,nClients1-1)
                while( (x = i+1) || (tempSet.Contains(x.ToString()))) do 
                    x <- rand1.Next(1,nActors)
                followingMap <-followingMap.Add(x.ToString(),followingMap.[x.ToString()].Add((i+1).ToString()))
                tempSet <- tempSet.Add(x.ToString())
                
                //followingList <- followingList.Add(ref.[i],)	
            followersMap <- followersMap.Add(reftoID.[ref.[i]], tempSet)
        
         
    
        //Threading.Thread.Sleep(100)


    let newTweetServerHandler(tweet:string)(userID:string) = 
        //printfn "Adding into datatables"
        tweetID <- tweetID + 1
        let dr = tweetTable.NewRow()
        dr.SetField("TweetID",(tweetID.ToString()))
        dr.SetField("Username", userID)
        dr.SetField("Tweet", tweet)
        //printfn "here reached too"
        tweetTable.Rows.Add(dr)
     
        Threading.Thread.Sleep(10)
        count <- count + 1
        if count%1000 = 0 then 
            printfn "Total tweets processed till now %i" count
        //printTweetTable tweet
        tweetParse (tweetID.ToString()) tweet userID
        findFollowers userID tweet


    let mentionServerHandler (userref:IActorRef) = 
        searchMentions userref
    //findFollowees "kartz"

    let newRetweetServerHandler(tweetID:string)(newUserID:string) = 
        retweetID <- retweetID + 1
        let dr = retweetTable.NewRow()
        dr.SetField("reTweetID",(retweetID.ToString()))
        dr.SetField("Username", newUserID)
        dr.SetField("TweetID", tweetID) 
        retweetTable.Rows.Add(dr)
        Threading.Thread.Sleep(10)
        count <- count + 1

        //printRetweetTable tweetID
        findFollowersForRetweeting newUserID tweetID
        Threading.Thread.Sleep(100)
        //printfn "in server handler function"


    let bossActor (mailbox: Actor<_>) =
        let rec loop () = actor {
            let! msg = mailbox.Receive() 
            match msg with 
            | SimulateFollowers (reflist,array,nActors,clients) ->
                //printfn "here"
                simulateFollowers reflist array nActors clients
            | AssignIDs(id,ref) ->
                assignIDs id ref
            | AddFollower(follower,followee) ->
                addFollowerServer follower followee
            | NewTweetServerHandler(tweet,userID) ->
                newTweetServerHandler tweet userID
            | RegisterServerHandler(uname,firstname,lastname,email,password) ->
                register uname firstname lastname email password
            | SearchHashtag hashtag ->
                searchHashtag hashtag
            | MentionServerHandler userref ->
                mentionServerHandler userref
            | KeywordSearchServerHandler(keyword,userref) ->
                searchKeyword keyword userref
            | NewRetweetServerHandler (tweetID,newUserID) ->
                newRetweetServerHandler tweetID newUserID
            | PrintHomeServerHandler userID ->
                printHomeServerHanlder userID  
            | SignMeOutServer userID ->
                signOffAUser userID
            | PrintPendingServerHanlder userID ->
                printPendingServerHanlder userID
            | SignMeInServer userID ->
                signInAUser userID

            return! loop ()
        }
        loop ()
        
    let serverRef = spawn system "boss" bossActor

// *************************************************************** TWITTER MAIN ***************************************************************

    let registrationMap = Map.empty


    //let system = System.create "system" (Configuration.defaultConfig())
    let array = Array.create 100 0
    let z = new Zipf(1.0,9)
    z.Samples(array)


    let addFollowerClient(follower:IActorRef)(followee:IActorRef) = 
        //printfn "reached here"
        serverRef <! AddFollower (follower,followee)

    let newTweetClientHandler(tweet:string)(userID:string) = 
        //printfn "calling server now"
        serverRef <! NewTweetServerHandler(tweet,userID)

    let newRetweetClientHandler(tweetID:string)(newUserID:string) = 
        //printfn "calling server for retweet now"
        serverRef <! NewRetweetServerHandler(tweetID, newUserID)
        Threading.Thread.Sleep(100)
        //printfn "completed?"

    let signMeInClientHandler (userID:string) =
        serverRef <! SignMeInServer userID


    let newUserRegisterClientHandler(uname:string)(firstname:string)(lastname:string)(email:string)(password:string) =
        serverRef <! RegisterServerHandler (uname,firstname,lastname,email,password)

    let searchHashtagClientHandler(hashtag:string) = 
        serverRef <! SearchHashtag hashtag

    let keywordSearchClientHandler(keyword:string)(userref:IActorRef) = 
        serverRef <! KeywordSearchServerHandler (keyword,userref)
     
    let mentionClientHandler(userref:IActorRef) = 
        serverRef <! MentionServerHandler userref 

    let signMeOutClientHandler (userID:string) =
        serverRef <! SignMeOutServer userID

    let printHomeClientHandler (userID:string) = 
        serverRef <! PrintHomeServerHandler userID

    let printPendingClientHandler(userID:string) = 
        serverRef <! PrintPendingServerHanlder userID

    let ClientActor (mailbox: Actor<_>) = 
        let rec loop () = actor {
            let mutable homeData:list<(string*string*string)> = list.Empty
            let! msg = mailbox.Receive()
            match msg with 
            | AddFollowerClient(follower,followee) ->
                addFollowerClient follower followee
            | SignMeIn userID ->
                signMeInClientHandler userID
            | NewTweetClientHandler(tweet,userID) ->
                //printfn "tweet handler"
                newTweetClientHandler tweet userID   
            //| UpdateHome ->
                //homeData <- homeMap.[mailbox.Self]
            | RegisterClientHandler(uname,firstname,lastname,email,password) ->
                newUserRegisterClientHandler uname firstname lastname email password
            | SearchHashtagClientHandler hashtag ->
                searchHashtagClientHandler hashtag
            | MentionClientHandler userref ->
                mentionClientHandler userref
            | KeywordSearchClientHandler (keyword,userref) ->
                keywordSearchClientHandler keyword userref
            | NewRetweetClientHandler (tweetID,newUserID) ->
                //printfn "retweet handler"
                newRetweetClientHandler tweetID newUserID
                //printfn "retweet handle ended"
            | SignMeOut userID ->
                signMeOutClientHandler userID
                // serverRef <! RegisterServerHandler(uname,firstname,lastname,email,password)
            | PrintHomeClientHandler userID ->
                printHomeClientHandler userID
            | PrintPendingClientHanlder userID ->
                printPendingClientHandler userID
            | Searchme searchstr ->
                printfn "In searchme"
                if(searchstr.[0] = '#' || searchstr.[0] ='@') then
                    if(searchstr.[0] ='#') then
                        searchHashtag searchstr mailbox.Self
                    if(searchstr.[0] ='@') then
                        printfn "IN @"
                        printfn "%A" IDtoref.[searchstr.[1..searchstr.Length]]
                        searchMentions IDtoref.[searchstr.[1..searchstr.Length]]
                else
                    searchKeyword searchstr mailbox.Self
                
            return! loop ()
        }
        loop ()

    let clientActor = spawn system "client" ClientActor

        

    Threading.Thread.Sleep(100)
    dt.AsEnumerable()
    |> Seq.iter(fun x -> printfn "%s" (String.Join(" ", x.ItemArray)))
    //let rand = Random(10)

// *************************************************************** SIMULATOR ***************************************************************


    let nActors = 100

    let childRefs = [for i in 1..nActors do yield (spawn system (sprintf "actor%i" i) ClientActor)]

    let nClients = (nActors*20)/100
    let array1 = Array.create nActors 0
    let z1 = new Zipf(1.0,nClients-1)
    z1.Samples(array1)

    let words = ["what";"some";"we";"can";"out";"other";"were";"all";"there";"white";"up";"used";"your";"how";"said";"an";"ear";"she";"which";"dog";"their";"time"]
    let rand = System.Random()
    let mutable IdsList = []


    let signinTesting(userID) = 
        let userRef = IDtoref.[userID]
        let array_ref = (userID |> int) - 1
        childRefs.[array_ref] <! SignMeIn userID

    let signoutTesting(userID) = 
        let userRef = IDtoref.[userID]
        let array_ref = (userID |> int) - 1
        childRefs.[array_ref] <! SignMeOut userID

    let printHome (userID:string) = 
        let userRef = IDtoref.[userID]
        let array_ref = (userID |> int) - 1
        childRefs.[array_ref] <! PrintHomeClientHandler userID

    let printPending(userID:string) = 
        let userRef = IDtoref.[userID]
        let array_ref = (userID |> int) - 1
        childRefs.[array_ref] <! PrintPendingClientHanlder userID


    // Threading.Thread.Sleep(10)

    // for i in 0..nActors-1 do 
      

    for i in 0..nActors-1 do 
        //registerMethod i childRefs.[i]
        //printfn "in the loop"
        assignIDs ((i+1).ToString()) childRefs.[i]

        childRefs.[i] <! RegisterClientHandler (((i+1)|>string ,"user" + (i+1).ToString(),"User" + (i+1).ToString(),"user" + (i+1).ToString() + "@gmail.com","123456"))
        //serverRef <! AssignIDs (((i+1).ToString()), childRefs.[i])
        //printfn "in the loop 2 "

        IdsList <- [(i+1).ToString()] |> List.append IdsList

    //printfn "server status before logging in %A" serverStatus
    let mutable t = true
    while (registerCount < nActors - 2) do
        t <- false
    printfn "Network completed"

    //Threading.Thread.Sleep(500)
    serverRef <! SimulateFollowers (childRefs,array1, nActors,nClients)

    //printfn "Simulator checkpoint 2"
    for i in 3..(nActors-1) do 
        //registerMethod i childRefs.[i]
        signinTesting ((i+1).ToString())

    //Threading.Thread.Sleep(1000)


    let addNewFollower (follower:IActorRef)(followee:IActorRef) = 
        //addFollowerClient follower followee
        clientActor <! AddFollowerClient (follower,followee)

    let newTweet(userID:string) =
        let choice = rand.Next(0, 3)
        let mutable sentence = ""
    //let choice = 2
        if choice = 0 then
            // simple sentence
            //let mutable sentence = ""
            for i in 1..6 do
                let word = rand.Next(0, words.Length-1)
                sentence <- sentence + " " + words.[word]
            //printfn "Case 0: %s" sentence
        elif choice = 1 then
            //let mutable sentence = ""
            for i in 1..6 do
                let word = rand.Next(0, words.Length-1)
                sentence <- sentence + " " + words.[word]
            let hashtag = rand.Next(words.Length-1)
            sentence <- sentence + " #" + words.[hashtag]
            //printfn "Case 1: %s" sentence
        elif choice = 2 then
            //let mutable sentence = ""
            for i in 1..6 do
                let word = rand.Next(0, words.Length-1)
                sentence <- sentence + " " + words.[word]
            let randNo = rand.Next(2)
            if randNo = 0 then 
                let hashtag = rand.Next(words.Length-1)
                sentence <- sentence + " #" + words.[hashtag]
                sentence <- sentence + " @" + IdsList.[rand.Next(IdsList.Length-1)]
                //printfn "Case 2.1: %s" sentence
            else
                sentence <- sentence + " @" + IdsList.[rand.Next(IdsList.Length-1)]
                //printfn "Case 2.2: %s" sentence

        //printfn "tweet is %s" sentence
        let userRef = IDtoref.[userID]
        let array_ref = (userID |> int) - 1
        childRefs.[array_ref] <! NewTweetClientHandler(sentence,userID)


    let newRetweet(tweetID:string)(newUserID:string) =
        let userRef = IDtoref.[newUserID]
        let array_ref = (newUserID |> int) - 1
        //printfn "everything good now?"
        childRefs.[array_ref] <! NewRetweetClientHandler(tweetID,newUserID)
        Threading.Thread.Sleep(100)
        //printfn "everything good?"
        


    // addNewFollower IDtoref.["1"] IDtoref.["5"]
    //addNewFollower IDtoref.["4"] IDtoref.["2"]

    Threading.Thread.Sleep(100)



    let mutable normalTweets = 0
    let mutable celebTweets = 0
    let stopwatch = System.Diagnostics.Stopwatch.StartNew()
    let mutable c = 0
    //printfn "Server status for 1 %A" serverStatus.[IDtoref.["1"]]
    for i in 3..nActors-1 do
        
        //if serverStatus.[IDtoref.[(i+1).ToString()]] then
            
        if array1.[i] < (nClients - nClients/10) then
            for j in 0..5 do 
                newTweet ((i+1).ToString())
                //Threading.Thread.Sleep(100)
                normalTweets <- normalTweets + 1
        else 
            for j in 0..5 do 
                newTweet ((i+1).ToString())
                celebTweets <- celebTweets + 1
                //Threading.Thread.Sleep(100)
            for j in 0..5 do
                newRetweet ((rand.Next(0,5)).ToString()) ((i+1).ToString())
                celebTweets <- celebTweets + 1

                //Threading.Thread.Sleep(100)
        
    Threading.Thread.Sleep(500)
                
    let totalTweets =  normalTweets + celebTweets
    //printfn "Total number of tweets are %i" (normalTweets + celebTweets)
    let mutable flag = true
    while (flag) do 
        if (count > totalTweets - 2)  then
            flag <- false
    stopwatch.Stop()

    printfn "Time taken for %i tweets is %f " totalTweets (stopwatch.Elapsed.TotalSeconds);

    Threading.Thread.Sleep(100)

    for i in 3..(nActors-1) do 
        //registerMethod i childRefs.[i]
        signoutTesting ((i+1).ToString())

    Threading.Thread.Sleep(100)
    //printfn "Server status for 4 %A" serverStatus.[IDtoref.["4"]]

    
    //printfn "%A "serverPendingMap

    for i in 0..3 do 
        //registerMethod i childRefs.[i]
        signinTesting ((i+1).ToString())

    // stopwatch.Reset()
    // stopwatch.Start()

    // // clientActor <! SearchHashtagClientHandler "#their"
    // Threading.Thread.Sleep(100)
    // stopwatch.Stop()
    // printfn "#########Time elapsed for hashtag search is %f ##################" stopwatch.Elapsed.TotalSeconds

    // printfn "##########result for mention search###########"
    // stopwatch.Reset()
    // stopwatch.Start()

    // //clientActor <! MentionClientHandler childRefs.[0]
    // Threading.Thread.Sleep(100)
    // stopwatch.Stop()
    // printfn "#########Time elapsed for Mention search is %f ##################" stopwatch.Elapsed.TotalSeconds


    // printfn "#########result for Keyword search#########"
    // stopwatch.Reset()
    // stopwatch.Start()
    // let keyword = "white"
    // //clientActor <! KeywordSearchClientHandler (keyword, childRefs.[0])
    // Threading.Thread.Sleep(10)
    // stopwatch.Stop()
    //printfn "#########Time elapsed for Keyword search for %s is %f ##################" keyword stopwatch.Elapsed.TotalSeconds


    startWebServer { defaultConfig with logger = Targets.create Verbose [||] } app
    0
