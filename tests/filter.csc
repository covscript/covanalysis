import analysis
var df = analysis.read_csv("cwurData.csv")
# system.out.println(df.find_broken_lines())
var result = df.columns("country").filter([](name)->name == "USA")
system.out.println(result)