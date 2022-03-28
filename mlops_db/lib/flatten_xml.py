import xml.etree.ElementTree as ET
import itertools
import pandas as pd

from typing import List, Dict, Tuple, Callable, Optional

from collections import Counter

Node = ET.Element

Row = Dict[str, str]


def _getMaybeRowTag(node : Node) -> Optional[str]:
  child_tags = [child_node.tag for child_node in node]
  child_tag_counts = Counter(child_tags)
  row_tags = [tag for tag, count in child_tag_counts.items() if count > 1]
  if len(row_tags) > 1:
    raise Exception(f"Multiple tags are candidates for rows: {row_tags}")
  try:
    row_tag = row_tags[0]
  except IndexError:
    row_tag = None
  return row_tag


def _toRow(node : Node) -> Row:
  row : Row = {}
  label = node.tag
  value = ""
  if node.text is not None:
    value = node.text
  row[label] = value
  return row


def _collapseDicts(dicts : Tuple[Dict[str, str]]) -> Dict[str, str]:
  out : Dict[str, str] = {}
  for t in dicts:
    out.update(t)
  return out

cartesianDicts = lambda terms : [_collapseDicts(t) for t in itertools.product(*terms)]


def toRows(node : Node, delim : str) -> List[Row]:
  join = lambda a, b : delim.join([a,b])

  rows : List[Row] = []
  if len(node) == 0:
    return [_toRow(node)]

  child_terms : List[List[Row]] = []
  terms       : List[List[Row]] = []

  row_tag = _getMaybeRowTag(node)

  top_level_tag = node.tag
  for child_node in node:
    maybe_rows = toRows(child_node, delim)
    if maybe_rows is not None:
      new_rows : List[Row] = []
      for row in maybe_rows:
        new_row = {join(top_level_tag, k) : v for k, v in row.items()}
        new_rows.append(new_row)
      if row_tag is not None and child_node.tag == row_tag:
        child_terms.append(new_rows)
      else:
        terms.append(new_rows)

  if len(child_terms):
    for child_term in child_terms:
      new_child_terms = terms.copy()
      new_child_terms.append(child_term)
      child_rows = cartesianDicts(new_child_terms)
      rows.extend(child_rows)
  else:
    rows = cartesianDicts(terms)

  return rows


#######
# CLI #
#######

if __name__ == "__main__":
  import argparse
  parser = argparse.ArgumentParser(description = 'Convert an xml document to a pandas dataframe')
  parser.add_argument("--filename", type=str, required=True)
  parser.add_argument("--delim", type=str, default="::")
  args = parser.parse_args()

  doc = ET.parse(args.filename)
  root_node = doc.getroot()
  rows = toRows(root_node, args.delim)
  data = pd.DataFrame.from_dict(rows)
  print("Dataframe: ",data.to_string())


#########
# Tests #
#########

def test_toRow():
  xml = """<foo>bar</foo>"""
  node = ET.fromstring(xml)
  row = _toRow(node)
  assert(row == {"foo": "bar"})


def test_toRowsSimple():
  xml = """<root><foo>bar</foo><fizz>buzz</fizz></root>"""
  node = ET.fromstring(xml)
  delim = "::"
  rows = toRows(node, delim)
  assert(rows == [{"root::foo": "bar", "root::fizz": "buzz"}])


def test_toRowsNested():
  xml = """<root>
             <first><foo>bar</foo></first>
             <second><fizz>buzz</fizz></second>
           </root>
        """
  node = ET.fromstring(xml)
  delim = "::"
  row = toRows(node, delim)
  assert(row == [{"root::first::foo": "bar", "root::second::fizz": "buzz"}])


def test_toRowsCartesian():
  xml = """<root>
             <shared>testing</shared>
             <nested><shared>hello</shared><shared2>goodbye</shared2></nested>
             <row>
               <first><foo>bar1</foo></first>
               <second><fizz>buzz1</fizz></second>
             </row>
             <row>
               <first><foo>bar2</foo></first>
               <second><fizz>buzz2</fizz></second>
             </row>
           </root>
        """
  node = ET.fromstring(xml)
  row = toRows(node, "::")

  assert(row == [{"root::shared": "testing",
                  "root::nested::shared": "hello",
                  "root::nested::shared2": "goodbye",
                  "root::row::first::foo": "bar1",
                  "root::row::second::fizz": "buzz1"},
                 {"root::shared": "testing",
                  "root::nested::shared": "hello",
                  "root::nested::shared2": "goodbye",
                  "root::row::first::foo": "bar2",
                  "root::row::second::fizz": "buzz2"}
                ])


def test_toRowsNestedCartesian():
  xml = """<root>
             <row>
               <first><foo>bar1</foo></first>
               <second><fizz>buzz1</fizz></second>
               <nested_row>
                 <animal>noise</animal>
                 <cats>meow</cats>
               </nested_row>
               <nested_row>
                 <animal>noise2</animal>
                 <cats>meow2</cats>
               </nested_row>
             </row>
             <row>
               <first><foo>bar2</foo></first>
               <second><fizz>buzz2</fizz></second>
             </row>
           </root>
        """
  node = ET.fromstring(xml)
  rows = toRows(node, "::")

  assert(rows == [{
                  "root::row::first::foo": "bar1",
                  "root::row::second::fizz": "buzz1",
                  "root::row::nested_row::animal": "noise",
                  "root::row::nested_row::cats": "meow"
                 },
                 {
                  "root::row::first::foo": "bar1",
                  "root::row::second::fizz": "buzz1",
                  "root::row::nested_row::animal": "noise2",
                  "root::row::nested_row::cats": "meow2"
                 },
                 {
                  "root::row::first::foo": "bar2",
                  "root::row::second::fizz": "buzz2",
                 }
                ])

def test_toRowsParallelCartesian():
  xml = """<root>
             <parallel>
               <foo>bar1</foo>
               <fizz><buzz>bazz1</buzz></fizz>
               <row>
                 <first><foo>bar1</foo></first>
                 <second><fizz>buzz1</fizz></second>
               </row>
               <row>
                 <first><foo>bar2</foo></first>
                 <second><fizz>buzz2</fizz></second>
               </row>
            </parallel>
            <parallel>
              <foo>bar2</foo>
              <fizz><buzz>bazz2</buzz></fizz>
              <row>
                <first><foo>bar3</foo></first>
                <second><fizz>buzz3</fizz></second>
              </row>
              <row>
                <first><foo>bar4</foo></first>
                <second><fizz>buzz4</fizz></second>
              </row>
           </parallel>
        </root>
       """
  node = ET.fromstring(xml)
  rows = toRows(node, "::")

  assert(rows == [{"root::parallel::foo": "bar1",
                   "root::parallel::fizz::buzz": "bazz1",
                   "root::parallel::row::first::foo": "bar1",
                   "root::parallel::row::second::fizz": "buzz1"
                  },
                  {"root::parallel::foo": "bar1",
                   "root::parallel::fizz::buzz": "bazz1",
                   "root::parallel::row::first::foo": "bar2",
                   "root::parallel::row::second::fizz": "buzz2"
                  },
                  {"root::parallel::foo": "bar2",
                   "root::parallel::fizz::buzz": "bazz2",
                   "root::parallel::row::first::foo": "bar3",
                   "root::parallel::row::second::fizz": "buzz3"
                  },
                  {"root::parallel::foo": "bar2",
                   "root::parallel::fizz::buzz": "bazz2",
                   "root::parallel::row::first::foo": "bar4",
                   "root::parallel::row::second::fizz": "buzz4"
                  }
                 ]
          )
