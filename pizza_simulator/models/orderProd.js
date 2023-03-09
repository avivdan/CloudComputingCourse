function getRandomInt(max) {
    return Math.floor(Math.random() * max);
}

function getBranchName(branch_id) {
    switch (branch_id) {
        case 1:
            return "papa_jones";
            break;
        case 2:
            return "dominos";
            break;
        case 3:
            return "hat";
            break;
        default:
            return "sod";
            break;
    }
}

function getAreaName(branch_id) {
    switch (branch_id) {
        case 1:
            return "Haifa";
        case 2:
            return "Mirkaz";
        case 3:
            return "Darom";
        default:
            return "Mizrah";
    }
}

function getRandomToppings(numberOfTopping) {
    var items = ["olives", " pepper", "mushrooms", "jalapinio", "onion"];
    var newItems = [];

    for (var i = 0; i < numberOfTopping; i++) {
        var idx = Math.floor(Math.random() * items.length);
        newItems.push(items[idx]);
        items.splice(idx, 1);
    }

    return newItems;
}


const generateOrder = () => {
    let b_id = getRandomInt(4);
    return {
        order_id: 5,
        branch_id: b_id,
        branch_name: getBranchName(b_id),
        area: getAreaName(b_id),
        Date: Date(),
        status: "proccessing",
        pizza_topping: getRandomToppings(2)
    };
};

module.exports = { generateOrder };