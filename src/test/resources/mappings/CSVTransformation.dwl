%dw 2.0

//With this annotation we make sure that we code in such a way the streaming is guaranteed
@StreamCapable
input payload application/csv streaming=true

// If deferred mode enable execution will be sent to another thread
// And the execution will return immediately. If not it will finish executing
// And if the output is bigger than a threshold 1.5 MB it will use disk as a buffer
//output application/csv deferred=true
output application/csv
---
payload
  filter ((user) -> user.UserAge > 37)
  map (user) -> {
    name: user.UserName,
    lastName: user.UserLastName
  }