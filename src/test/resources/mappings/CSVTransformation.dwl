%dw 2.0

@StreamCapable
input payload application/csv streaming=true

output application/csv
---
payload
  filter ((user) -> user.UserAge > 37)
  map (user) -> {
    name: user.UserName,
    lastName: user.UserLastName
  }