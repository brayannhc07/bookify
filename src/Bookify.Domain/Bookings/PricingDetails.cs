using Bookify.Domain.Apartments;

namespace Bookify.Domain.Bookings;

public record PricingDetails(
    Money PriceForPerdiod,
    Money CleaningFee,
    Money AmenitiesUpCharge,
    Money TotalPrice);