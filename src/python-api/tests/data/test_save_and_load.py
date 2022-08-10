# Copyright 2022 The SQLNet Company GmbH
# 
# This file is licensed under the Elastic License 2.0 (ELv2). 
# Refer to the LICENSE.txt file in the root of the repository 
# for details.
# 

import getml


def load_if_needed(name):
    """
    Loads the data from the relational learning
    repository, if the data frame has not already
    been loaded.
    """
    if not getml.data.exists(name):
        data_frame = getml.DataFrame.from_db(name=name, table_name=name)
        data_frame.save()
    else:
        data_frame = getml.data.load_data_frame(name)
    return data_frame


def make_target_columns(data_frame, class_label):
    for label in class_label:
        name = "class_label=" + label
        data_frame[name] = data_frame.class_label == label
        data_frame.set_role(name, getml.data.roles.target)
    data_frame.set_role("class_label", getml.data.roles.unused_string)


def test_save_and_load():
    getml.engine.set_project("cora")

    getml.database.connect_mariadb(
        host="relational.fit.cvut.cz",
        dbname="CORA",
        port=3306,
        user="guest",
        password="relational",
    )

    paper = load_if_needed("paper")
    cites = load_if_needed("cites")
    content = load_if_needed("content")

    paper.set_role("paper_id", getml.data.roles.join_key)
    paper.set_role("class_label", getml.data.roles.categorical)

    cites.set_role(["cited_paper_id", "citing_paper_id"], getml.data.roles.join_key)

    content.set_role("paper_id", getml.data.roles.join_key)
    content.set_role("word_cited_id", getml.data.roles.categorical)

    split = getml.data.split.random(train=0.7, test=0.3, validation=0.0)

    data_train = paper[split == "train"].to_df("data_train")
    data_test = paper[split == "test"].to_df("data_test")

    class_label = paper.class_label.unique()

    make_target_columns(data_train, class_label)
    make_target_columns(data_test, class_label)

    container = getml.data.Container(train=data_train, test=data_test[:800])
    container.add(paper=paper, cites=cites, content=content)
    container.freeze()

    container.save()

    container2 = getml.data.load_container(container.id)

    container2.train
    container2.test
    container2.split


if __name__ == "__main__":
    test_save_and_load()
