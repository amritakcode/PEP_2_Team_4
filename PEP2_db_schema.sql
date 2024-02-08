use cardio;

create table habits(
	habit_id int primary key,
    smoke int,
    alcohol int,
    activ int,
    cardio int
);

create table blood_info(
	blood_id int primary key,
    ap_hi int,
    ap_low int,
    cholesterol int,
    glucose int
);

create table patient(
    patient_id int primary key,
    age int,
    gender int,
    height int,
    weight float,
    habit_id int,
    blood_id int,
    foreign key(habit_id) references habits(habit_id),
    foreign key(blood_id) references blood_info(blood_id)
);
