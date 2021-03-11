%dw 2.0

input payload application/csv streaming=true

output application/csv
---
payload
  map (user) -> {
    name: user.UserName,
    lastName: user.UserLastName
  }