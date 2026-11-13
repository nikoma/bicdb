CREATE OPERATOR adempiere.- (
    FUNCTION = adempiere.subtractdays,
    LEFTARG = interval,
    RIGHTARG = numeric,
    COMMUTATOR = OPERATOR(adempiere.-)
);
