using Bookify.Application.Abstractions.Messaging;

namespace Bookify.Application.Bookings.GetQueryBooking;

public record GetBookingQuery(Guid BookingId) : IQuery<BookingResponse>;