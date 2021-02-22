//To execute this script just do
//This will generate data
//dw -f src/scripts/SampleGenerator.dwl -o src/test/resources/csv_big/Users.csv

%dw 2.0

output application/csv
var names = [

  "Mariano",
  "Leandro",
  "Agustin",
  "Ana",
  "Ignacio",
  "Teodor",
  "Matias",
  "Christian",
  "Emiliano"
]

var lastName = [

  "Achaval",
  "Shokida",
  "Mendez",
  "Felissati",
  "Ponisio",
  "Hekear",
  "Chubaka",
  "Chibana",
  "Lesende"
]
fun randomName(): String =
  names[randomInt(sizeOf(names))]

fun randomLastName(): String =
  lastName[randomInt(sizeOf(lastName))]

fun randomAge(): Number =
 randomInt(100)

---
1 to 100000 map {
  UserName: randomName(),
  UserLastName: randomLastName(),
  UserAge: randomAge()
}