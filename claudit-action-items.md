# claudit-mutators item 2

Validate table name is a table in the zero schema. Presumably permissions already handles this though.

# claudit-mutators item 4

No rate limiting for custom mutators. Zero could do this with rate limit config... More ideally API server returns a special error that causes zero-cache to bork the user.


# claudit-protocol item 1

Logs auth tokens!

# claudit-protocol item 2

Document footgun of url patterns that are too wide.

# claudit-protocol item 3

Cookie forward default to false? So no credential exfil via 3 + 2.

# claudit-queries item 3

Hash cache key so we do not accidentally log it

# claudit-queries item 4

Less revealing error messages in prod?

# claudit-replication item 4

Create a schema for the sec protocol to validate what we decode

# claudit-replication item 6

ALLOWED_APP_ID_CHARACTERS should exclude sql injection characters

# claudit-replication item 7

appID is interpolated without escaping

# claudit-replication 9

negative numbers

# claudit-websocket item 3

no origin validation on connections

The Attack Scenario:                                                                                                      
                                                                                                                            
  1. User logs into your Zero-powered app at yourapp.com (which sets an auth cookie)                                        
  2. User visits evil.com in another tab (while still logged in)                                                            
  3. evil.com runs JavaScript: new WebSocket('wss://yourapp.com/sync')                                                      
  4. Browser automatically includes the user's yourapp.com cookies with the WebSocket handshake                             
  5. If the server doesn't check the Origin header, it accepts the connection                                               
  6. evil.com can now sync data as the authenticated user                                                                   
                                                                                                                            
  Why WebSockets are special:                                                                                               
                                                                                                                            
  Regular HTTP requests are protected by CORS - the browser blocks cross-origin responses unless the server explicitly      
  allows it. But WebSocket handshakes bypass CORS. The browser sends an Origin header, but it's up to the server to reject  
  bad origins. If the server ignores it, the connection succeeds.                                                           
                                                                                                                            
  When this applies:                                                                                                        
                                                                                                                            
  Only when using cookie-based auth (httpCookie in Zero config). Token-based auth in headers isn't vulnerable because       
  evil.com can't access or send those tokens.                                                                               
                                                                                                                            
  The fix:                                                                                                                  
                                                                                                                            
  On WebSocket upgrade, check that the Origin header matches your expected domain(s):                                       
                                                                                                                            
  if (request.headers.origin !== 'https://yourapp.com') {                                                                   
    socket.destroy();                                                                                                       
    return;                                                                                                                 
  }                    

# claudit-websocket item 6

no connection limits per ip / user

# claudit-websocket item 8

no json size limits when parsing

# claudit-websocket item 9

jwks cached without ttl

# claudit-zserver item 1

 Schema Parameter Insufficient Validation

# claudit-zserver item 2

error info disclosure


# claudit-zserver item 3

debug logging of mutation args

# Token Expiration NOT Enforced Post-Connection  
                                                                                       
Status: VULNERABILITY (Medium Severity)                                                                                   
                                                                                                                          
Finding: JWT expiration (exp claim) is only validated at WebSocket connection time. Once connected, the token is stored   
and used for authorization without re-validation.                                                                         
                                                                                                                          
Evidence:                                                                                                                 
                                                                                                                          
1. Token verified at connection (syncer.ts:166-168):                                                                      
decodedToken = await verifyToken(this.#config.auth, auth, {                                                               
  subject: userID,                                                                                                        
});                                                                                                                       
                                                                                                                          
2. Token stored for connection lifetime (syncer.ts:211-214):                                                              
auth ? {                                                                                                                  
  raw: auth,                                                                                                              
  decoded: decodedToken ?? {},                                                                                            
} : undefined,                                                                                                            
                                                                                                                          
3. Stored token used for authorization without expiration check (view-syncer.ts:1167, view-syncer.ts:1363):               
this.#authData?.decoded,  // Used directly for permission checks                                                          
                                                                                                                          
Impact: A user with an expired token maintains full access until they disconnect. Long-lived WebSocket connections        
(potentially hours or days) can operate with stale authorization.                                                         
                                                                                                                          
Recommendation: Implement periodic token expiration checking during active connections.

# Claim Validation

Recommendation: Add issuer and audience configuration options to enable full claim validation.   



----


- json depth?



  1. NODE_ENV=production disables JSON depth assertions                                                                                                                                                                                                     
  2. No explicit JSON nesting limit (tested to 5000 levels)                                                                                                                                                                                                 
  3. No size limit on base64 tokens in WebSocket protocol header                                                                                                                                                                                            
  4. Unicode identifiers not NFC-normalized     