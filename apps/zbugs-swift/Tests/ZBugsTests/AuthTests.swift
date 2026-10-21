import Testing
import ZBugs

@Suite struct AuthTests {
  // {"alg":"EdDSA"}.{"sub":"u-1","role":"crew","name":"ann","iat":1,"exp":2000}.sig
  static let token =
    "eyJhbGciOiJFZERTQSJ9.eyJzdWIiOiJ1LTEiLCJyb2xlIjoiY3JldyIsIm5hbWUiOiJhbm4iLCJpYXQiOjEsImV4cCI6MjAwMH0.c2ln"

  @Test func readsTheClaimsOfAToken() {
    #expect(AuthData(jwt: Self.token, now: 1999) == AuthData(sub: "u-1", role: .crew))
  }

  @Test func rejectsAnExpiredOrMalformedToken() {
    #expect(AuthData(jwt: Self.token, now: 2001) == nil)
    #expect(AuthData(jwt: "a.b", now: 0) == nil)
    #expect(AuthData(jwt: "a.!!.c", now: 0) == nil)
  }
}
