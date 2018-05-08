require "./crystal-playground/*"
require "./store"

class Main
  def run
    store = Store::Store.new

    store.put("hi", "there")
    store.put("hi::123", "sup")
    store.put("zzz", "")
    store.prefix("hi").each do |k, v|
      puts "#{k} => #{v}"
    end

    s = Store::Set.new(store, "fun-things")
    s.add("parties")
    s.add("snow")
    s.add("money")
    p s.all
    s.add("snow")
    s.add("food")
    p s.all
    s.remove("parties", "snow", "money", "food")
    p s.all
    s.add("parties", "snow", "money", "food")

    bad = Store::Set.new(store, "bad-things")
    bad.add("taxes")
    bad.add("regulations")
    bad.add("money")
    bad.add("snow")

    puts "--- union"
    p s.rewind.union(bad).to_a
    puts "--- intersection"
    p s.intersection(bad).to_a

    store.close
  end
end

main = Main.new
main.run
