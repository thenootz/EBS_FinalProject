package ebs.common;

import ebs.proto.EbsProto.Predicate;
import ebs.proto.EbsProto.Publication;
import ebs.proto.EbsProto.Subscription;

import java.time.LocalDate;
import java.util.List;

/**
 * Evaluates whether a Publication satisfies the predicates of a Subscription.
 *
 * Each predicate has the form (field, operator, value).
 * Supported operators: =, !=, <, <=, >, >=
 */
public final class Matcher {

    private Matcher() {}

    /** Returns true if the publication satisfies ALL predicates of the subscription. */
    public static boolean matches(Publication pub, Subscription sub) {
        for (Predicate pred : sub.getPredicatesList()) {
            if (!matchesPredicate(pub, pred)) return false;
        }
        return true;
    }

    /** Returns true if the publication satisfies the single predicate. */
    public static boolean matchesPredicate(Publication pub, Predicate pred) {
        String field = pred.getField();
        String op    = pred.getOperator();
        String val   = pred.getValue();

        return switch (field) {
            case "company"   -> compareString(pub.getCompany(),   op, val);
            case "value"     -> compareDouble(pub.getValue(),     op, val);
            case "drop"      -> compareDouble(pub.getDrop(),      op, val);
            case "variation" -> compareDouble(pub.getVariation(), op, val);
            case "date"      -> compareDate  (pub.getDate(),      op, val);
            default          -> false;
        };
    }

    // ── String comparison (= and != only) ────────────────────────────────────
    private static boolean compareString(String pubVal, String op, String subVal) {
        // strip quotes if present
        String clean = subVal.replace("\"", "");
        return switch (op) {
            case "="  -> pubVal.equalsIgnoreCase(clean);
            case "!=" -> !pubVal.equalsIgnoreCase(clean);
            default   -> false;
        };
    }

    // ── Double comparison ─────────────────────────────────────────────────────
    private static boolean compareDouble(double pubVal, String op, String subVal) {
        try {
            double sv = Double.parseDouble(subVal.replace("\"", ""));
            return switch (op) {
                case "="  -> pubVal == sv;
                case "!=" -> pubVal != sv;
                case ">"  -> pubVal >  sv;
                case ">=" -> pubVal >= sv;
                case "<"  -> pubVal <  sv;
                case "<=" -> pubVal <= sv;
                default   -> false;
            };
        } catch (NumberFormatException e) {
            return false;
        }
    }

    // ── Date comparison ───────────────────────────────────────────────────────
    private static boolean compareDate(String pubDate, String op, String subDate) {
        try {
            LocalDate pd = LocalDate.parse(pubDate);
            LocalDate sd = LocalDate.parse(subDate.replace("\"", ""));
            int cmp = pd.compareTo(sd);
            return switch (op) {
                case "="  -> cmp == 0;
                case "!=" -> cmp != 0;
                case ">"  -> cmp >  0;
                case ">=" -> cmp >= 0;
                case "<"  -> cmp <  0;
                case "<=" -> cmp <= 0;
                default   -> false;
            };
        } catch (Exception e) {
            return false;
        }
    }

    /**
     * Returns the subset of predicates from a subscription that this broker
     * is responsible for (based on which fields are in the given list).
     */
    public static List<Predicate> filterPredicatesForFields(
            Subscription sub, java.util.Set<String> ownedFields) {
        return sub.getPredicatesList().stream()
                  .filter(p -> ownedFields.contains(p.getField()))
                  .toList();
    }
}
