#include <covscript/cni.hpp>
#include <covscript/dll.hpp>
#include "csv2.hpp"

CNI_ROOT_NAMESPACE {
	using namespace cs;
	var read_csv(const std::string& path)
	{
		csv2::Reader<csv2::delimiter<','>, csv2::quote_character<'"'>, csv2::first_row_is_header<false>, csv2::trim_policy::trim_characters<' ', '\t', '\r'>> csv;
		if (csv.mmap(path)) {
			var data = var::make<array>();
			array& arr = data.val<array>();
			for (const auto row: csv) {
				if (row.length() != 0) {
					var row_data = var::make<array>();
					array& row_arr = row_data.val<array>();
					for (const auto cell: row) {
						var cell_data = var::make<cs::string>();
						cell.read_value(cell_data.val<cs::string>());
						row_arr.emplace_back(std::move(cell_data));
					}
					arr.emplace_back(std::move(row_data));
				}
			}
			return std::move(data);
		}
		else
			return cs::null_pointer;
	}
	CNI(read_csv)
	bool write_csv(const array& data, const std::string& path)
	{
		std::ofstream stream(path);
		if (!stream)
			return false;
		csv2::Writer<csv2::delimiter<','>> writer(stream);
		for (auto &line : data) {
			const array& line_arr = line.const_val<array>();
			std::vector<std::string> data;
			for (auto &val : line_arr)
				data.emplace_back(val.to_string());
			writer.write_row(data);
		}
		return true;
	}
	CNI(write_csv)
	array select(const array& data, const array& cols)
	{
		cs::array arr;
		for (auto &line : data) {
			var new_line_var = var::make<array>();
			array& new_line = new_line_var.val<array>();
			const array& line_arr = line.const_val<array>();
			for (auto &col : cols)
				new_line.emplace_back(line_arr.at(col.const_val<numeric>().as_integer()));
			arr.emplace_back(new_line_var);
		}
		return move(arr);
	}
	CNI(select)
	array filter(const array& data, const array& cols, const var& func)
	{
		cs::array arr;
		if (func.type() == typeid(callable)) {
			const callable& cond = func.const_val<callable>();
			for (auto &line : data) {
				vector args;
				const array& line_arr = line.const_val<array>();
				for (auto &col : cols)
					args.emplace_back(line_arr.at(col.const_val<numeric>().as_integer()));
				if (cond.call(args).const_val<bool>())
					arr.emplace_back(line);
			}
		}
		else if (func.type() == typeid(object_method)) {
			const auto &om = func.const_val<object_method>();
			const callable& cond = om.callable.const_val<callable>();
			for (auto &line : data) {
				vector args{om.object};
				const array& line_arr = line.const_val<array>();
				for (auto &col : cols)
					args.emplace_back(line_arr.at(col.const_val<numeric>().as_integer()));
				if (cond.call(args).const_val<bool>())
					arr.emplace_back(line);
			}
		}
		else
			throw runtime_error("Invoke non-callable object.");
		return std::move(arr);
	}
	CNI(filter)
	hash_map groupby(const array& data, std::size_t column)
	{
		hash_map map;
		for (std::size_t i = 0; i < data.size(); ++i) {
			const var& key = data[i].const_val<array>()[column];
			if (map.count(key) == 0)
				map.emplace(key, var::make<array>());
			map[key].val<array>().emplace_back(var::make<numeric>(i));
		}
		return std::move(map);
	}
	CNI(groupby)
	hash_map groupby_group(const array& data, const array &group, std::size_t column)
	{
		hash_map map;
		for (std::size_t i = 0; i < group.size(); ++i) {
			std::size_t idx = group[i].const_val<numeric>().as_integer();
			const var& key = data[idx].const_val<array>()[column];
			if (map.count(key) == 0)
				map.emplace(key, var::make<array>());
			map[key].val<array>().emplace_back(var::make<numeric>(idx));
		}
		return std::move(map);
	}
	CNI(groupby_group)
}